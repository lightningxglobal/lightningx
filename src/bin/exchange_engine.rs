use aeron_wrapper::AeronClient;
use lightning_exchange::{
    account_repository::AccountRepository,
    aeron_channels::{
        DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM, METRICS_CHANNEL, METRICS_STREAM,
        ORDER_UPDATE_STREAM_BASE, TRADE_STREAM, aeron_dir, depth_channel, order_update_channel,
        order_update_stream_for_desk, orders_channel, orders_stream_for_symbol, trade_channel,
    },
    aeron_transport::{
        AeronMarketDataPublisher, AeronOrderSubscriber, AeronOrderUpdatePublisher,
        AeronTradePublisher,
    },
    db,
    engine::{MatchingEngine, OrderStatus, PoolConfig},
    market_data::DepthSnapshotEvent,
    models::DbOrder,
    order::{Order, Side, TimeInForce},
    sbe::TradeNotification,
    symbol_rules::SymbolRules,
    tracer::{ENGINE_INSTANCE_ID, spawn_tracer},
    transport::{
        InboundMsg, MarketDataPublisher, OrderSubscriber, OrderUpdateMsg, OrderUpdatePublisher,
        TradePublisher,
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn symbol_bytes(symbol: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let bytes = symbol.as_bytes();
    let copy_len = bytes.len().min(16);
    out[..copy_len].copy_from_slice(&bytes[..copy_len]);
    out
}

fn symbol_from_bytes(bytes: &[u8; 16]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(16);
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

fn response_stream_count() -> u16 {
    std::env::var("ORDER_UPDATE_STREAM_COUNT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(lightning_exchange::desk::counter_shard::COUNTER_SHARD_COUNT)
}

fn response_stream_index(stream_id: i32, publishers_len: usize) -> usize {
    if stream_id >= ORDER_UPDATE_STREAM_BASE {
        let idx = (stream_id - ORDER_UPDATE_STREAM_BASE) as usize;
        if idx < publishers_len {
            return idx;
        }
    }
    0
}

// Self-trade prevention is intentionally NOT done in the matching hot path.
// Compliance runs its own offline, non-realtime surveillance; the engine's
// job is to attach sufficient metadata to every trade (both user ids are
// denormalized onto the trades table via TradeInsertPayload). A realtime
// book scan per inbound order would be O(crossing orders) — unacceptable.

/// Reference price for the band check, O(1):
/// prefer the book mid when both sides are present and sane, otherwise fall
/// back to the last trade price so an empty/one-sided book (open, liquidity
/// drought — exactly when protection matters most) is still covered.
fn band_reference_ticks(engine: &MatchingEngine, last_trade_ticks: i64) -> Option<i128> {
    if let (Some(best_bid), Some(best_ask)) =
        (engine.best_price_ticks(true), engine.best_price_ticks(false))
    {
        if best_bid > 0 && best_ask > 0 && best_bid <= best_ask {
            return Some((best_bid as i128 + best_ask as i128) / 2);
        }
    }
    if last_trade_ticks > 0 {
        return Some(last_trade_ticks as i128);
    }
    None
}

fn outside_price_band(
    engine: &MatchingEngine,
    side: Side,
    price_ticks: i64,
    band_bps: u32,
    last_trade_ticks: i64,
) -> bool {
    if price_ticks <= 0 || band_bps == 0 {
        return false;
    }
    let Some(reference) = band_reference_ticks(engine, last_trade_ticks) else {
        return false;
    };
    let band = reference * band_bps as i128 / 10_000;
    match side {
        Side::Buy => (price_ticks as i128) > reference + band,
        Side::Sell => (price_ticks as i128) < reference - band,
    }
}

/// Trading-halt circuit breaker, O(1) per trade and per order.
///
/// Tracks the first trade price of a rolling window; if any trade within
/// the window deviates more than `threshold_bps` from the window-open
/// price, the symbol enters cancel-only mode for `cooldown_ns`. New orders
/// are rejected (reject_reason 10) while halted; cancels always pass so
/// participants can pull quotes. `threshold_bps == 0` disables.
struct CircuitBreaker {
    threshold_bps: u32,
    window_ns: u64,
    cooldown_ns: u64,
    window_start_ns: u64,
    window_open_ticks: i64,
    halted_until_ns: u64,
}

impl CircuitBreaker {
    fn new(threshold_bps: u32, window_ns: u64, cooldown_ns: u64) -> Self {
        Self {
            threshold_bps,
            window_ns,
            cooldown_ns,
            window_start_ns: 0,
            window_open_ticks: 0,
            halted_until_ns: 0,
        }
    }

    #[inline]
    fn is_halted(&self, now_ns: u64) -> bool {
        now_ns < self.halted_until_ns
    }

    /// Record a trade. Returns true when this trade newly trips the breaker.
    fn on_trade(&mut self, now_ns: u64, price_ticks: i64) -> bool {
        if self.threshold_bps == 0 || price_ticks <= 0 {
            return false;
        }
        if self.window_open_ticks <= 0 || now_ns.saturating_sub(self.window_start_ns) > self.window_ns
        {
            self.window_start_ns = now_ns;
            self.window_open_ticks = price_ticks;
            return false;
        }
        let open = self.window_open_ticks as i128;
        let dev = (price_ticks as i128 - open).abs() * 10_000;
        if dev > open * self.threshold_bps as i128 {
            self.halted_until_ns = now_ns + self.cooldown_ns;
            // Re-anchor so post-halt trading measures from the halt price,
            // not the pre-crash level (which would re-trip instantly).
            self.window_start_ns = now_ns;
            self.window_open_ticks = price_ticks;
            return true;
        }
        false
    }
}

/// Per-order size limits, O(1). `max_lots`/`max_notional` of 0 disable the
/// respective check. Notional is in ticks×lots units (i128, overflow-free).
/// Market orders (price 0) are only bounded by lots.
fn exceeds_order_limits(
    price_ticks: i64,
    quantity_lots: i64,
    max_lots: i64,
    max_notional: i128,
) -> bool {
    if max_lots > 0 && quantity_lots > max_lots {
        return true;
    }
    if max_notional > 0 && price_ticks > 0 {
        let notional = price_ticks as i128 * quantity_lots as i128;
        if notional > max_notional {
            return true;
        }
    }
    false
}

/// Per-participant open-order accounting, O(1) per transition. Counts are
/// kept exactly in sync with uid_map insert/remove so a cap can be enforced
/// without scanning anything.
#[inline]
fn open_count_inc(counts: &mut HashMap<u64, u32>, participant_id: u64) {
    if participant_id != 0 {
        *counts.entry(participant_id).or_insert(0) += 1;
    }
}

#[inline]
fn open_count_dec(counts: &mut HashMap<u64, u32>, participant_id: u64) {
    if participant_id == 0 {
        return;
    }
    if let Some(c) = counts.get_mut(&participant_id) {
        *c = c.saturating_sub(1);
        if *c == 0 {
            counts.remove(&participant_id);
        }
    }
}

fn publish_order_update(
    publishers: &mut [AeronOrderUpdatePublisher],
    sequences: &mut [u64],
    stream_id: i32,
    msg: &OrderUpdateMsg,
) {
    let idx = response_stream_index(stream_id, publishers.len());
    let mut sequenced = *msg;
    if let Some(seq) = sequences.get_mut(idx) {
        *seq += 1;
        sequenced.sequence = *seq;
    }
    let _ = publishers[idx].publish(&sequenced);
}

fn env_core_at(name: &str, index: usize) -> Option<usize> {
    let value = std::env::var(name).ok()?;
    let mut cores = value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<usize>().ok());
    cores.nth(index)
}

#[cfg(target_os = "linux")]
fn pin_current_thread_to_core(name: &str, index: usize, label: &str) {
    let Some(core) = env_core_at(name, index) else {
        return;
    };
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        let rc = libc::pthread_setaffinity_np(
            libc::pthread_self(),
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        );
        if rc == 0 {
            tracing::info!("{label} pinned to cpu {core} via {name}[{index}]");
        } else {
            tracing::warn!(
                "{label} failed to pin to cpu {core} via {name}[{index}]: {}",
                std::io::Error::from_raw_os_error(rc)
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread_to_core(name: &str, index: usize, label: &str) {
    if let Some(core) = env_core_at(name, index) {
        tracing::warn!(
            "{label} requested cpu pin {core} via {name}[{index}], but this platform does not support pthread affinity"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightning_exchange::engine::PoolConfig;

    #[test]
    fn price_band_rejects_only_aggressive_extremes() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine
            .place_order(Order::new(1, Side::Buy, 90, 5, TimeInForce::GTC, 0))
            .unwrap();
        engine
            .place_order(Order::new(2, Side::Sell, 110, 5, TimeInForce::GTC, 0))
            .unwrap();

        assert!(outside_price_band(&engine, Side::Buy, 250, 10_000, 0));
        assert!(outside_price_band(&engine, Side::Sell, 49, 5_000, 0));
        assert!(!outside_price_band(&engine, Side::Buy, 50, 10_000, 0));
        assert!(!outside_price_band(&engine, Side::Sell, 250, 10_000, 0));
        assert!(!outside_price_band(&engine, Side::Sell, 0, 10_000, 0));
        assert!(!outside_price_band(&engine, Side::Sell, -1, 10_000, 0));
        assert!(!outside_price_band(&engine, Side::Buy, 250, 0, 0));
    }

    #[test]
    fn price_band_falls_back_to_last_trade_when_book_is_empty() {
        let engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Empty book + no last trade → no reference, no protection (cold start).
        assert_eq!(band_reference_ticks(&engine, 0), None);
        assert!(!outside_price_band(&engine, Side::Buy, 1_000_000, 10_000, 0));

        // Empty book + last trade at 100 → band anchors on the last trade.
        assert_eq!(band_reference_ticks(&engine, 100), Some(100));
        assert!(outside_price_band(&engine, Side::Buy, 201, 10_000, 100));
        assert!(!outside_price_band(&engine, Side::Buy, 200, 10_000, 100));
        assert!(outside_price_band(&engine, Side::Sell, 49, 5_000, 100));
        assert!(!outside_price_band(&engine, Side::Sell, 50, 5_000, 100));
    }

    #[test]
    fn price_band_falls_back_when_book_is_one_sided() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine
            .place_order(Order::new(1, Side::Buy, 90, 5, TimeInForce::GTC, 0))
            .unwrap();

        // Only a bid in the book → mid is undefined → last trade anchors.
        assert_eq!(band_reference_ticks(&engine, 100), Some(100));
        assert!(outside_price_band(&engine, Side::Buy, 201, 10_000, 100));
    }

    #[test]
    fn price_band_prefers_book_mid_over_last_trade() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine
            .place_order(Order::new(1, Side::Buy, 90, 5, TimeInForce::GTC, 0))
            .unwrap();
        engine
            .place_order(Order::new(2, Side::Sell, 110, 5, TimeInForce::GTC, 0))
            .unwrap();

        // Stale last trade at 10_000 must NOT widen the band: mid (100) wins.
        assert_eq!(band_reference_ticks(&engine, 10_000), Some(100));
        assert!(outside_price_band(&engine, Side::Buy, 250, 10_000, 10_000));
    }

    #[test]
    fn circuit_breaker_trips_on_extreme_move_and_recovers() {
        const MS: u64 = 1_000_000;
        // 10% threshold, 10s window, 60s cooldown.
        let mut cb = CircuitBreaker::new(1_000, 10_000 * MS, 60_000 * MS);

        // First trade anchors the window; no trip.
        assert!(!cb.on_trade(0, 100));
        // 9.99% move within window: no trip.
        assert!(!cb.on_trade(1_000 * MS, 109));
        assert!(!cb.is_halted(1_001 * MS));
        // 11% move within window: trip.
        assert!(cb.on_trade(2_000 * MS, 111));
        assert!(cb.is_halted(2_001 * MS));
        assert!(cb.is_halted(2_000 * MS + 59_999 * MS));
        // Cooldown elapsed: trading resumes.
        assert!(!cb.is_halted(2_000 * MS + 60_000 * MS));
        // Post-halt anchor is the halt price (111): a return to 100 (~9.9%)
        // does NOT instantly re-trip.
        assert!(!cb.on_trade(63_000 * MS, 100));
    }

    #[test]
    fn circuit_breaker_window_expiry_reanchors() {
        const MS: u64 = 1_000_000;
        let mut cb = CircuitBreaker::new(1_000, 10_000 * MS, 60_000 * MS);
        assert!(!cb.on_trade(0, 100));
        // 50% move but AFTER the window expired → re-anchor, no trip.
        // (Slow drifts are legitimate; the breaker targets sudden moves.)
        assert!(!cb.on_trade(20_000 * MS, 150));
        // Now a fast 11% move from the new anchor trips.
        assert!(cb.on_trade(21_000 * MS, 167));
    }

    #[test]
    fn circuit_breaker_disabled_never_trips() {
        let mut cb = CircuitBreaker::new(0, 1, 1);
        assert!(!cb.on_trade(0, 100));
        assert!(!cb.on_trade(1, 100_000));
        assert!(!cb.is_halted(2));
    }

    #[test]
    fn order_limits_enforce_lots_and_notional() {
        // Disabled limits pass everything.
        assert!(!exceeds_order_limits(1_000_000, 1_000_000, 0, 0));
        // Lots cap applies to limit and market orders alike.
        assert!(exceeds_order_limits(100, 11, 10, 0));
        assert!(exceeds_order_limits(0, 11, 10, 0)); // market order
        assert!(!exceeds_order_limits(100, 10, 10, 0));
        // Notional cap (ticks × lots) applies to priced orders only.
        assert!(exceeds_order_limits(100, 11, 0, 1_000));
        assert!(!exceeds_order_limits(100, 10, 0, 1_000));
        assert!(!exceeds_order_limits(0, 1_000_000, 0, 1_000)); // market: lots-only
        // i128 product cannot overflow even at i64 extremes.
        assert!(exceeds_order_limits(i64::MAX, i64::MAX, 0, 1));
    }

    #[test]
    fn open_order_counts_track_inserts_and_removes_exactly() {
        let mut counts: HashMap<u64, u32> = HashMap::new();
        open_count_inc(&mut counts, 42);
        open_count_inc(&mut counts, 42);
        open_count_inc(&mut counts, 7);
        assert_eq!(counts.get(&42), Some(&2));
        assert_eq!(counts.get(&7), Some(&1));

        open_count_dec(&mut counts, 42);
        assert_eq!(counts.get(&42), Some(&1));
        open_count_dec(&mut counts, 42);
        assert_eq!(counts.get(&42), None, "zero entries are evicted");
        // Underflow and unknown ids are no-ops, not panics.
        open_count_dec(&mut counts, 42);
        open_count_dec(&mut counts, 999);
        // participant 0 (unattributed) is never tracked.
        open_count_inc(&mut counts, 0);
        assert!(!counts.contains_key(&0));
    }
}

/// Spawn one fully-independent matching thread for a single symbol.
///
/// Each thread creates its own AeronClient connection — this avoids the 3rd
/// publication registration hanging when all symbols share one client.
fn spawn_symbol_thread(
    core_index: usize,
    symbol: String,
    engine: MatchingEngine,
    tracer: Option<lightning_exchange::tracer::ExchangeTracer>,
    uid_map: HashMap<u64, (u64, i32)>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("match-{}", symbol))
        .spawn(move || {
            pin_current_thread_to_core("ENGINE_MATCH_CORES", core_index, "matching");
            let client = Arc::new(AeronClient::new(&aeron_dir()).expect("AeronClient"));
            let orders_stream = orders_stream_for_symbol(&symbol);
            let mut subscriber =
                AeronOrderSubscriber::new(client.clone(), &orders_channel(), orders_stream)
                    .unwrap_or_else(|e| panic!("[{}] subscriber: {}", symbol, e));
            let response_count = response_stream_count();
            let mut ou_pubs: Vec<AeronOrderUpdatePublisher> = (0..response_count)
                .map(|desk_id| {
                    let stream = order_update_stream_for_desk(desk_id);
                    AeronOrderUpdatePublisher::new(client.clone(), &order_update_channel(), stream)
                        .unwrap_or_else(|e| {
                            panic!("[{}] ou_pub stream {}: {}", symbol, stream, e)
                        })
                })
                .collect();
            let mut ou_seqs = vec![0_u64; ou_pubs.len()];
            let mut trade_pub =
                AeronTradePublisher::new(client.clone(), &trade_channel(), TRADE_STREAM)
                    .unwrap_or_else(|e| panic!("[{}] trade_pub: {}", symbol, e));
            let mut md_pub = AeronMarketDataPublisher::new(
                client.clone(),
                &depth_channel(),
                DEPTH_STREAM,
                DEPTH50_STREAM,
                LEVEL2_STREAM,
            )
            .unwrap_or_else(|e| panic!("[{}] md_pub: {}", symbol, e));

            let mut engine = engine;
            let rules = SymbolRules::for_symbol(&symbol);
            // order_id → participant_id: populated on ACCEPTED, used on CANCEL to route
            // the update back to the correct user even when cancel doesn't carry uid.
            let mut uid_map = uid_map;
            let mut trade_seq: u64 = 0;
            let mut depth_seq: u64 = 0;
            let depth_interval = Duration::from_millis(10);
            let mut last_depth = Instant::now();
            let sym_bytes_fixed = symbol_bytes(&symbol);

            tracing::info!("[{}] matching thread started", symbol);

            // Burst telemetry: how many msgs per poll-batch + how long the
            // batch took. Logged every 10s so we can tell whether tail p99 is
            // dominated by big bursts (MM 20-quote cancel-replace) or by
            // long per-msg matching, without adding more beacon milestones.
            let mut stats_iters: u64 = 0;
            let mut stats_msgs: u64 = 0;
            let mut stats_burst_max: u32 = 0;
            let mut stats_burst_5: u64 = 0;
            let mut stats_burst_10: u64 = 0;
            let mut stats_burst_20: u64 = 0;
            let mut stats_dur_sum_us: u64 = 0;
            let mut stats_dur_max_us: u64 = 0;
            let mut stats_last = Instant::now();
            // Matching is latency-critical: default to infinite spin so a
            // warm order stream does not wait behind a sleep wake-up. Shared
            // dev hosts can opt into bounded spinning with ENGINE_IDLE_SPINS.
            //
            // ENGINE_IDLE_SPINS=0 or unset means infinite spin. A positive
            // value spins that many idle loops before sleeping 1-50µs.
            let idle_spin_budget: Option<u32> = match std::env::var("ENGINE_IDLE_SPINS")
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(0u32) => None,
                Some(n) => Some(n),
                None => None,
            };
            let mut idle_iters: u32 = 0;
            let mut idle_sleep_us: u64 = 1;
            // Band width: per-symbol rules value, overridable engine-wide
            // via ENGINE_PRICE_BAND_BPS.
            let price_band_bps = std::env::var("ENGINE_PRICE_BAND_BPS")
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(rules.price_band_bps);
            // Circuit breaker: halt (cancel-only) on extreme intra-window
            // moves. ENGINE_CB_BPS=0 (default) disables.
            let cb_bps = std::env::var("ENGINE_CB_BPS")
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0);
            let cb_window_ms = std::env::var("ENGINE_CB_WINDOW_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(10_000);
            let cb_cooldown_ms = std::env::var("ENGINE_CB_COOLDOWN_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60_000);
            let mut breaker =
                CircuitBreaker::new(cb_bps, cb_window_ms * 1_000_000, cb_cooldown_ms * 1_000_000);
            // Per-order / per-participant limits. 0 disables a limit.
            let max_order_lots = std::env::var("ENGINE_MAX_ORDER_LOTS")
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            let max_order_notional = std::env::var("ENGINE_MAX_ORDER_NOTIONAL")
                .ok()
                .and_then(|s| s.parse::<i128>().ok())
                .unwrap_or(0);
            let max_open_orders = std::env::var("ENGINE_MAX_OPEN_ORDERS_PER_USER")
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0);
            tracing::info!(
                "[{}] limits: band_bps={} max_order_lots={} max_order_notional={} max_open_orders={}",
                symbol, price_band_bps, max_order_lots, max_order_notional, max_open_orders,
            );
            // O(1) per-participant resting-order counts, kept in lockstep
            // with uid_map (see open_count_inc/dec).
            let mut open_counts: HashMap<u64, u32> = HashMap::new();
            // Band fallback anchor when the book is empty/one-sided.
            let mut last_trade_ticks: i64 = 0;

            loop {
                // Only the subscriber needs do_work() — it processes the incoming IPC ring.
                // IPC publishers write directly to mapped memory; no do_work() needed.
                subscriber.do_work();

                let batch_start = Instant::now();
                let mut batch_count: u32 = 0;
                while let Some(msg) = subscriber.poll() {
                    batch_count += 1;
                    // Refresh Aeron client heartbeat every 256 msgs so a
                    // big burst (e.g. MM cancel-replace × 40 quotes × N
                    // re-quotes) can't keep the matching thread inside
                    // this loop for >10 s — which kills the AeronClient
                    // conductor (we measured 245 K-msg batches → 18 s
                    // process time → "service interval exceeded").
                    // do_work() is a cheap pointer + counter update on
                    // the conductor heartbeat; ~50 ns when nothing to do.
                    if batch_count & 0xff == 0 {
                        client.do_work();
                    }
                    match msg {
                        InboundMsg::NewOrder(req) => {
                            let req_symbol = symbol_from_bytes(&req.symbol);
                            let response_stream_id = req.response_stream_id;
                            if req_symbol != symbol {
                                let client_order_id = req.client_order_id;
                                let participant_id = req.participant_id;
                                tracing::warn!(
                                    "[{}] rejecting order {} for mismatched symbol {}",
                                    symbol,
                                    client_order_id,
                                    req_symbol,
                                );
                                publish_order_update(
                                    &mut ou_pubs,
                                    &mut ou_seqs,
                                    response_stream_id,
                                    &OrderUpdateMsg::rejected(
                                        client_order_id,
                                        participant_id,
                                        4,
                                        now_ns(),
                                    ),
                                );
                                continue;
                            }
                            // if let Some(ref t) = tracer {
                            //     t.record_sym(MS_AERON_ORDER_RECV, req.client_order_id, &req.symbol);
                            // }
                            let ts = now_ns();
                            let side = if req.side == 0 { Side::Buy } else { Side::Sell };
                            let tif = match req.time_in_force {
                                0 => TimeInForce::GTC,
                                1 => TimeInForce::IOC,
                                2 => TimeInForce::FOK,
                                3 => TimeInForce::PostOnly,
                                _ => TimeInForce::GTC,
                            };
                            let quantity_lots = req.quantity_lots;
                            if quantity_lots <= 0 {
                                publish_order_update(
                                    &mut ou_pubs,
                                    &mut ou_seqs,
                                    response_stream_id,
                                    &OrderUpdateMsg::rejected(
                                        req.client_order_id,
                                        req.participant_id,
                                        2,
                                        ts,
                                    ),
                                );
                                continue;
                            }
                            if breaker.is_halted(ts) {
                                // Cancel-only mode: reject new orders during
                                // the cooldown; cancels are handled below.
                                publish_order_update(
                                    &mut ou_pubs,
                                    &mut ou_seqs,
                                    response_stream_id,
                                    &OrderUpdateMsg::rejected(
                                        req.client_order_id,
                                        req.participant_id,
                                        10,
                                        ts,
                                    ),
                                );
                                continue;
                            }
                            if outside_price_band(
                                &engine,
                                side,
                                req.price_ticks,
                                price_band_bps,
                                last_trade_ticks,
                            ) {
                                publish_order_update(
                                    &mut ou_pubs,
                                    &mut ou_seqs,
                                    response_stream_id,
                                    &OrderUpdateMsg::rejected(
                                        req.client_order_id,
                                        req.participant_id,
                                        7,
                                        ts,
                                    ),
                                );
                                continue;
                            }
                            // reject_reason 6 (was realtime STP) is retired:
                            // self-trade handling moved to offline compliance
                            // surveillance over trade metadata.
                            if exceeds_order_limits(
                                req.price_ticks,
                                quantity_lots,
                                max_order_lots,
                                max_order_notional,
                            ) {
                                publish_order_update(
                                    &mut ou_pubs,
                                    &mut ou_seqs,
                                    response_stream_id,
                                    &OrderUpdateMsg::rejected(
                                        req.client_order_id,
                                        req.participant_id,
                                        8,
                                        ts,
                                    ),
                                );
                                continue;
                            }
                            let participant_id: u64 = req.participant_id;
                            if max_open_orders > 0
                                && participant_id != 0
                                && open_counts
                                    .get(&participant_id)
                                    .is_some_and(|&c| c >= max_open_orders)
                            {
                                publish_order_update(
                                    &mut ou_pubs,
                                    &mut ou_seqs,
                                    response_stream_id,
                                    &OrderUpdateMsg::rejected(
                                        req.client_order_id,
                                        req.participant_id,
                                        9,
                                        ts,
                                    ),
                                );
                                continue;
                            }
                            let order = if req.price_ticks == 0 {
                                Order::new_market(req.client_order_id, side, quantity_lots, ts)
                            } else {
                                Order::new(
                                    req.client_order_id,
                                    side,
                                    req.price_ticks,
                                    quantity_lots,
                                    tif,
                                    ts,
                                )
                            };

                            match engine.place_order(order) {
                                Err(e) => {
                                    tracing::warn!("[{}] place_order failed: {:?}", symbol, e);
                                    // if let Some(ref t) = tracer {
                                    //     t.record_sym(
                                    //         MS_MATCHING_DONE,
                                    //         req.client_order_id,
                                    //         &req.symbol,
                                    //     );
                                    // }
                                    publish_order_update(
                                        &mut ou_pubs,
                                        &mut ou_seqs,
                                        response_stream_id,
                                        &OrderUpdateMsg::rejected(
                                            req.client_order_id,
                                            req.participant_id,
                                            2,
                                            ts,
                                        ),
                                    );
                                    // if let Some(ref t) = tracer {
                                    //     t.record_sym(
                                    //         MS_AERON_UPDATE_SEND,
                                    //         req.client_order_id,
                                    //         &req.symbol,
                                    //     );
                                    // }
                                }
                                Ok(result) => {
                                    // if let Some(ref t) = tracer {
                                    //     t.record_sym(
                                    //         MS_MATCHING_DONE,
                                    //         req.client_order_id,
                                    //         &req.symbol,
                                    //     );
                                    // }
                                    let last_price = result
                                        .fills
                                        .last()
                                        .map(|f| rules.ticks_to_price(f.1))
                                        .unwrap_or_else(|| rules.ticks_to_price(req.price_ticks));
                                    let filled_qty = rules.lots_to_quantity(result.filled_lots);
                                    let total_qty = rules.lots_to_quantity(req.quantity_lots);
                                    let update = match result.status {
                                        OrderStatus::Accepted => {
                                            if uid_map
                                                .insert(
                                                    result.order_id,
                                                    (req.participant_id, response_stream_id),
                                                )
                                                .is_none()
                                            {
                                                open_count_inc(
                                                    &mut open_counts,
                                                    req.participant_id,
                                                );
                                            }
                                            OrderUpdateMsg::accepted(
                                                result.order_id,
                                                req.client_order_id,
                                                req.participant_id,
                                                ts,
                                            )
                                        }
                                        OrderStatus::Filled => {
                                            if let Some((pid, _)) =
                                                uid_map.remove(&result.order_id)
                                            {
                                                open_count_dec(&mut open_counts, pid);
                                            }
                                            OrderUpdateMsg::filled(
                                                result.order_id,
                                                req.client_order_id,
                                                req.participant_id,
                                                last_price,
                                                filled_qty,
                                                ts,
                                            )
                                        }
                                        OrderStatus::PartiallyFilled => {
                                            // Order rests in book after partial fill — must track
                                            // participant so cancel confirmations route correctly.
                                            if uid_map
                                                .insert(
                                                    result.order_id,
                                                    (req.participant_id, response_stream_id),
                                                )
                                                .is_none()
                                            {
                                                open_count_inc(
                                                    &mut open_counts,
                                                    req.participant_id,
                                                );
                                            }
                                            OrderUpdateMsg::partial_fill(
                                                result.order_id,
                                                req.client_order_id,
                                                req.participant_id,
                                                last_price,
                                                filled_qty,
                                                total_qty - filled_qty,
                                                ts,
                                            )
                                        }
                                        OrderStatus::Cancelled => {
                                            if let Some((pid, _)) =
                                                uid_map.remove(&result.order_id)
                                            {
                                                open_count_dec(&mut open_counts, pid);
                                            }
                                            OrderUpdateMsg::cancelled(
                                                result.order_id,
                                                req.client_order_id,
                                                req.participant_id,
                                                total_qty - filled_qty,
                                                ts,
                                            )
                                        }
                                        OrderStatus::Rejected => OrderUpdateMsg::rejected(
                                            req.client_order_id,
                                            req.participant_id,
                                            3,
                                            ts,
                                        ),
                                    };
                                    // Publish trades before the terminal order update so desk can
                                    // settle while runtime order metadata is still live.
                                    for &(maker_order_id, fill_price_ticks, fill_qty_lots) in
                                        &result.fills
                                    {
                                        trade_seq += 1;
                                        last_trade_ticks = fill_price_ticks;
                                        if breaker.on_trade(ts, fill_price_ticks) {
                                            tracing::warn!(
                                                "[{}] CIRCUIT BREAKER tripped at {} ticks — cancel-only for {}ms",
                                                symbol, fill_price_ticks, cb_cooldown_ms,
                                            );
                                        }
                                        let fill_price = rules.ticks_to_price(fill_price_ticks);
                                        let fill_qty = rules.lots_to_quantity(fill_qty_lots);
                                        let trade = TradeNotification {
                                            sequence: trade_seq,
                                            taker_order_id: req.client_order_id,
                                            maker_order_id,
                                            price: fill_price,
                                            quantity: fill_qty,
                                            side: req.side,
                                            _pad: [0; 7],
                                            symbol: sym_bytes_fixed,
                                        };
                                        let _ = trade_pub.publish(&trade);
                                        // GC routing metadata for makers this fill fully
                                        // consumed (the engine has already evicted them).
                                        // O(1) per fill; also fixes uid_map growing
                                        // unboundedly with filled maker entries.
                                        if !engine.contains_order(maker_order_id)
                                            && let Some((pid, _)) = uid_map.remove(&maker_order_id)
                                        {
                                            open_count_dec(&mut open_counts, pid);
                                        }
                                    }
                                    publish_order_update(
                                        &mut ou_pubs,
                                        &mut ou_seqs,
                                        response_stream_id,
                                        &update,
                                    );
                                    // if let Some(ref t) = tracer {
                                    //     t.record_sym(
                                    //         MS_AERON_UPDATE_SEND,
                                    //         req.client_order_id,
                                    //         &req.symbol,
                                    //     );
                                    // }
                                }
                            }
                        }

                        InboundMsg::CancelOrder(req) => {
                            let ts = now_ns();
                            let cancel_oid: u64 = req.order_id;
                            let request_stream_id = req.response_stream_id;
                            match engine.cancel_order(cancel_oid) {
                                Ok(res) => {
                                    // Prefer uid_map; fall back to participant_id in the request
                                    // (covers ghost orders never inserted into uid_map).
                                    let removed = uid_map.remove(&cancel_oid);
                                    if let Some((pid, _)) = removed {
                                        open_count_dec(&mut open_counts, pid);
                                    }
                                    let (participant_id, response_stream_id) =
                                        removed.unwrap_or((req.participant_id, request_stream_id));
                                    publish_order_update(
                                        &mut ou_pubs,
                                        &mut ou_seqs,
                                        response_stream_id,
                                        &OrderUpdateMsg::cancelled(
                                            req.order_id,
                                            0,
                                            participant_id,
                                            rules.lots_to_quantity(res.cancelled_quantity),
                                            ts,
                                        ),
                                    );
                                }
                                Err(_) => {
                                    // Order not in this engine (wrong symbol or already gone).
                                    // Send CANCELED with qty=0 so the caller can unblock.
                                    // Use participant_id from the request — uid_map won't have it.
                                    let participant_id = req.participant_id;
                                    if participant_id != 0 {
                                        publish_order_update(
                                            &mut ou_pubs,
                                            &mut ou_seqs,
                                            request_stream_id,
                                            &OrderUpdateMsg::cancelled(
                                                req.order_id,
                                                0,
                                                participant_id,
                                                0.0,
                                                ts,
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                // Burst telemetry: only count non-empty batches so the avg
                // reflects real work, not idle spin loops.
                if batch_count > 0 {
                    let dur_us = batch_start.elapsed().as_micros() as u64;
                    stats_iters += 1;
                    stats_msgs += batch_count as u64;
                    if batch_count > stats_burst_max { stats_burst_max = batch_count; }
                    if batch_count >= 5 { stats_burst_5 += 1; }
                    if batch_count >= 10 { stats_burst_10 += 1; }
                    if batch_count >= 20 { stats_burst_20 += 1; }
                    stats_dur_sum_us += dur_us;
                    if dur_us > stats_dur_max_us { stats_dur_max_us = dur_us; }
                }
                if stats_last.elapsed() >= Duration::from_secs(10) {
                    if stats_iters > 0 {
                        let dur_avg = stats_dur_sum_us / stats_iters;
                        let msgs_per_iter = stats_msgs as f64 / stats_iters as f64;
                        tracing::info!(
                            "[{}] burst stats (10s): iters={} msgs={} msgs/iter={:.1} burst_max={} ≥5={} ≥10={} ≥20={} batch_dur_us avg={} max={}",
                            symbol, stats_iters, stats_msgs, msgs_per_iter,
                            stats_burst_max, stats_burst_5, stats_burst_10, stats_burst_20,
                            dur_avg, stats_dur_max_us,
                        );
                    }
                    stats_iters = 0; stats_msgs = 0; stats_burst_max = 0;
                    stats_burst_5 = 0; stats_burst_10 = 0; stats_burst_20 = 0;
                    stats_dur_sum_us = 0; stats_dur_max_us = 0;
                    stats_last = Instant::now();
                }

                // Depth snapshot every 10ms.
                let now = Instant::now();
                let mut did_work = batch_count > 0;
                if now.duration_since(last_depth) >= depth_interval {
                    last_depth = now;
                    depth_seq += 1;
                    let mut snap = DepthSnapshotEvent::new(now_ns(), depth_seq);
                    snap.symbol = sym_bytes_fixed;
                    let mut bids = [(0, 0); 20];
                    let mut asks = [(0, 0); 20];
                    let num_bids = engine.fill_top_levels(true, &mut bids);
                    let num_asks = engine.fill_top_levels(false, &mut asks);
                    for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
                        snap.bids[i] = (rules.ticks_to_price(*p), rules.lots_to_quantity(*q));
                    }
                    for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
                        snap.asks[i] = (rules.ticks_to_price(*p), rules.lots_to_quantity(*q));
                    }
                    snap.num_bids = num_bids as u8;
                    snap.num_asks = num_asks as u8;
                    if num_bids > 0 && num_asks > 0 {
                        let _ = md_pub.publish_depth(&snap);
                        did_work = true;
                    }
                }

                if did_work {
                    idle_iters = 0;
                    idle_sleep_us = 1;
                } else if idle_spin_budget
                    .map(|budget| idle_iters < budget)
                    .unwrap_or(true)
                {
                    if idle_spin_budget.is_some() {
                        idle_iters += 1;
                    }
                    std::hint::spin_loop();
                } else {
                    std::thread::sleep(Duration::from_micros(idle_sleep_us));
                    idle_sleep_us = (idle_sleep_us * 2).min(50);
                }
            }
        })
        .expect("failed to spawn symbol thread")
}

// Engine matching loop runs on its own std::thread per symbol (see
// spawn_symbol_thread). Tokio is only used here for sqlx startup queries
// and ctrl-c handling — `current_thread` skips spinning up 14 idle worker
// threads that sit parked the entire process lifetime.
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    lightning_exchange::util::install_panic_hook();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());

    let symbols: Vec<String> = std::env::var("SYMBOLS")
        .unwrap_or_else(|_| "ETH_USDT,BTC_USDT,SOL_USDT".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    tracing::info!("Connecting to database...");
    let pool = db::create_pool(&database_url).await?;
    tracing::info!("DB connected");

    // Cancel stale bot orders from previous session.
    {
        let repo = AccountRepository::new(&pool);
        let bot_stale: Vec<(i64, i64, String, String, f64, f64, f64)> = sqlx::query_as(
            "SELECT o.id, o.user_id, o.symbol, o.side, o.quantity, o.filled, COALESCE(o.freeze_price, COALESCE(o.price, 0.0))
             FROM orders o
             JOIN users u ON u.id = o.user_id
             WHERE o.status IN ('PENDING','TRADING')
               AND u.email IN ('robot@lightningx.exchange', 'demo@lightning.exchange')",
        )
        .fetch_all(&pool).await.unwrap_or_default();

        // Release frozen funds per order (must be per-row: different amounts per user/asset),
        // then cancel all stale bot orders in one bulk UPDATE instead of N individual queries.
        for (_, user_id, symbol, side, quantity, filled, per_unit_price) in &bot_stale {
            let remaining = quantity - filled;
            if remaining > 0.0 {
                let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
                let base_asset = sym_parts.first().copied().unwrap_or("BTC");
                let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
                if side == "sell" {
                    let _ = repo.release_frozen(*user_id, base_asset, remaining).await;
                } else if *per_unit_price > 0.0 {
                    let _ = repo
                        .release_frozen(*user_id, quote_asset, per_unit_price * remaining)
                        .await;
                }
            }
        }
        if !bot_stale.is_empty() {
            let ids: Vec<i64> = bot_stale.iter().map(|(id, ..)| *id).collect();
            let _ = sqlx::query(
                "UPDATE orders SET status='CANCELED', updated_at=NOW()
                 WHERE id = ANY($1)",
            )
            .bind(&ids)
            .execute(&pool)
            .await;
            tracing::warn!(
                "Canceled {} stale bot orders from previous session",
                bot_stale.len()
            );
        }
    }

    // Restore active limit orders from DB into per-symbol engines.
    let mut engines: HashMap<String, MatchingEngine> = symbols
        .iter()
        .map(|s| {
            let eng = MatchingEngine::new(PoolConfig::default()).expect("engine");
            (s.clone(), eng)
        })
        .collect();

    // uid_maps: symbol → (order_id → participant_id), pre-seeded from restored DB orders
    // so that cancel events for restored orders carry the correct participant_id.
    let mut uid_maps: HashMap<String, HashMap<u64, (u64, i32)>> = HashMap::new();

    let rows = sqlx::query_as::<_, DbOrder>(
        "SELECT * FROM orders WHERE status IN ('PENDING', 'TRADING') ORDER BY id ASC",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let mut restored = 0usize;
    let mut skipped = 0usize;
    for db_order in &rows {
        if db_order.order_type == "market"
            || (db_order.order_type == "ioc" && db_order.price.is_none())
        {
            skipped += 1;
            continue;
        }
        let remaining = db_order.quantity - db_order.filled;
        if remaining <= 0.0 {
            continue;
        }
        let side = if db_order.side == "buy" {
            Side::Buy
        } else {
            Side::Sell
        };
        let rules = SymbolRules::for_symbol(&db_order.symbol);
        let price_ticks = match rules.price_to_ticks(db_order.price.unwrap_or(0.0)) {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let quantity_lots = match rules.quantity_to_lots(remaining) {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let order = Order::new(
            db_order.id as u64,
            side,
            price_ticks,
            quantity_lots,
            TimeInForce::GTC,
            0,
        );
        if let Some(eng) = engines.get_mut(&db_order.symbol) {
            if eng.add_to_book(order).is_ok() {
                uid_maps.entry(db_order.symbol.clone()).or_default().insert(
                    db_order.id as u64,
                    (db_order.user_id as u64, order_update_stream_for_desk(0)),
                );
                restored += 1;
            }
        }
    }
    tracing::info!(
        "Restored {} active orders ({} non-restable skipped)",
        restored,
        skipped
    );

    // Detect crossed books after restoration: can happen if the engine crashed between
    // desk-server writing an order to DB and the engine receiving it via Aeron.
    // If bid >= ask for any symbol, cancel all orders for that symbol and rebuild fresh.
    {
        let repo = AccountRepository::new(&pool);
        let mut crossed_fixed = 0usize;
        for (sym, eng) in engines.iter_mut() {
            let bids = eng.get_top_levels(1, true);
            let asks = eng.get_top_levels(1, false);
            let crossed = matches!((bids.first(), asks.first()),
                (Some(&(bid, _)), Some(&(ask, _))) if bid >= ask);
            if !crossed {
                continue;
            }

            let rules = SymbolRules::for_symbol(sym);
            let best_bid = bids
                .first()
                .map(|&(p, _)| rules.ticks_to_price(p))
                .unwrap_or(0.0);
            let best_ask = asks
                .first()
                .map(|&(p, _)| rules.ticks_to_price(p))
                .unwrap_or(f64::MAX);
            tracing::warn!(
                "Crossed book for {} (bid={} >= ask={}) — cancelling all open orders and rebuilding",
                sym,
                best_bid,
                best_ask
            );

            #[allow(clippy::type_complexity)]
            let open_orders: Vec<(i64, i64, String, f64, f64, Option<f64>, f64)> = sqlx::query_as(
                "SELECT id, user_id, side, quantity, filled, price, freeze_price
                     FROM orders WHERE symbol=$1 AND status IN ('PENDING','TRADING')",
            )
            .bind(sym.as_str())
            .fetch_all(&pool)
            .await
            .unwrap_or_default();

            let sym_parts: Vec<&str> = sym.splitn(2, '_').collect();
            let base_asset = sym_parts.first().copied().unwrap_or("BTC");
            let quote_asset = sym_parts.last().copied().unwrap_or("USDT");

            for (id, user_id, side, quantity, filled, price, freeze_price) in &open_orders {
                let remaining = quantity - filled;
                if remaining > 0.0 {
                    if side == "sell" {
                        let _ = repo.release_frozen(*user_id, base_asset, remaining).await;
                    } else {
                        let fp = if *freeze_price > 0.0 {
                            Some(*freeze_price)
                        } else {
                            price.filter(|&p| p > 0.0)
                        };
                        if let Some(p) = fp {
                            let _ = repo
                                .release_frozen(*user_id, quote_asset, p * remaining)
                                .await;
                        }
                    }
                }
                let _ = sqlx::query(
                    "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1",
                )
                .bind(id)
                .execute(&pool)
                .await;
            }

            *eng = MatchingEngine::new(PoolConfig::default()).expect("engine");
            uid_maps.remove(sym.as_str());
            crossed_fixed += open_orders.len();
        }
        if crossed_fixed > 0 {
            tracing::warn!(
                "Fixed crossed book(s): cancelled {} orders total",
                crossed_fixed
            );
        }
    }

    // Cancel stale market orders from crash.
    {
        let repo = AccountRepository::new(&pool);
        let stale: Vec<(i64, i64, String, String, f64, f64, Option<f64>, f64)> = sqlx::query_as(
            "SELECT id, user_id, symbol, side, quantity, filled, price, freeze_price
             FROM orders WHERE status IN ('PENDING','TRADING') AND order_type='market'",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let mut cleaned = 0usize;
        for (id, user_id, symbol, side, quantity, filled, row_price, freeze_price) in stale {
            let remaining = quantity - filled;
            if remaining > 0.0 {
                let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
                let base_asset = sym_parts.first().copied().unwrap_or("BTC");
                let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
                if side == "sell" {
                    let _ = repo.release_frozen(user_id, base_asset, remaining).await;
                } else {
                    let p = if freeze_price > 0.0 {
                        Some(freeze_price)
                    } else {
                        row_price.filter(|p| *p > 0.0)
                    };
                    if let Some(p) = p {
                        let _ = repo
                            .release_frozen(user_id, quote_asset, p * remaining)
                            .await;
                    }
                }
            }
            let _ =
                sqlx::query("UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1")
                    .bind(id)
                    .execute(&pool)
                    .await;
            cleaned += 1;
        }
        if cleaned > 0 {
            tracing::warn!("Cleaned up {} stale market orders", cleaned);
        }
    }

    drop(pool);

    tracing::info!(
        "Exchange engine started — spawning {} symbol threads",
        symbols.len()
    );

    // ----- Latency tracer (optional, gated by TRACER_ENABLED env) ------------
    // Off by default. At 400K conns the live tracer (3 record_sym per msg)
    // pushed matching-thread batch_dur from ~4µs to ~276ms — the SystemTime
    // calls + unbounded mpsc sends + cache-line invalidation crushed the
    // hot path. Cheap `if let Some` guards remain on every call site so
    // turning it back on with `TRACER_ENABLED=1` requires no code changes.
    let tracer = if std::env::var("TRACER_ENABLED")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        spawn_tracer(
            &aeron_dir(),
            METRICS_CHANNEL,
            METRICS_STREAM,
            ENGINE_INSTANCE_ID,
        )
    } else {
        None
    };
    if tracer.is_some() {
        tracing::info!(
            "Exchange tracer connected (instance_id={})",
            ENGINE_INSTANCE_ID
        );
    }

    // ----- Spawn one independent matching thread per symbol ------------------
    // Each thread creates its own AeronClient so publications don't contend.
    let mut handles = Vec::new();
    for (idx, symbol) in symbols.iter().enumerate() {
        let engine = engines.remove(symbol).expect("engine missing");
        let uid_map = uid_maps.remove(symbol).unwrap_or_default();
        let handle = spawn_symbol_thread(idx, symbol.clone(), engine, tracer.clone(), uid_map);
        handles.push(handle);
    }

    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down exchange engine...");

    Ok(())
}
