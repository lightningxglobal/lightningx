use aeron_wrapper::AeronClient;
use dashmap::DashMap;
use lightning_exchange::{
    aeron_channels::{
        DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM, METRICS_CHANNEL, METRICS_STREAM,
        PERSIST_CHANNEL, PERSIST_STREAM, TRADE_STREAM, aeron_dir, counter_forward_channel,
        counter_forward_cmd_stream_for_desk, counter_forward_resp_stream_for_desk, depth_channel,
        order_update_channel, order_update_stream_for_desk, orders_channel,
        orders_stream_for_symbol, trade_channel,
    },
    aeron_transport::{
        CounterForwardPublisher, CounterForwardSubscriber, DeskDepthSubscriber, DeskOrderPublisher,
        DeskOrderUpdateSubscriber, DeskTradeSubscriber, PersistPublisher,
    },
    api::{AccountCache, AppState, router},
    db,
    money::{AccountBalance, AmountAtoms},
    order_state::{DbOrderStatus, db_status_from_update_kind, ws_status_from_update_kind},
    tracer::{
        DESK_INSTANCE_ID,
        MS_LIQ_FILL_RECV,
        MS_LIQ_ORDER_SENT,
        // MS_CMD_RING_POPPED, MS_AERON_ORDER_SEND, MS_AERON_UPDATE_RECV,
        // MS_USER_TX_SENT, MS_WS_UPDATE_SEND,  // uncomment to enable 6-gap breakdown
        MS_LIQ_TICK_EMIT,
        spawn_tracer,
    },
    transport::counter_forward::{
        CounterForwardMsg, CounterForwardOrderMeta, CounterForwardWsFrame,
    },
    transport::persist_event::{
        AccountSetPayload, MatchingEventPayload, OrderDeletePayload, OrderFillUpdatePayload,
        OrderUpsertPayload, PersistFrame, TradeInsertPayload, matching_event_kind, pack_str,
    },
    transport::{AeronCmd, unpack_str16},
    ws_handler::market_data_broadcaster,
};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};

// ── DB status byte constants ──────────────────────────────────────────────────
mod db_cmd {
    use lightning_exchange::DbOrderStatus;

    pub fn status_str(status: u8) -> &'static str {
        match status {
            0 => DbOrderStatus::Pending.as_str(),
            1 => DbOrderStatus::Trading.as_str(),
            2 => DbOrderStatus::Completed.as_str(),
            3 => DbOrderStatus::Canceled.as_str(),
            _ => DbOrderStatus::Rejected.as_str(),
        }
    }

    pub fn str_bytes<const N: usize>(s: &str) -> [u8; N] {
        let mut buf = [0u8; N];
        let b = s.as_bytes();
        let n = b.len().min(N);
        buf[..n].copy_from_slice(&b[..n]);
        buf
    }
}

#[derive(Clone, Copy)]
struct LiveTradePoint {
    ts_secs: i64,
    price: f64,
    qty: f64,
}

#[derive(Clone, Copy)]
struct LiveBucket {
    start: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    trade_count: u64,
}

impl LiveBucket {
    fn new(start: i64, price: f64, qty: f64) -> Self {
        Self {
            start,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: qty,
            trade_count: 1,
        }
    }

    fn update(&mut self, price: f64, qty: f64) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.close = price;
        self.volume += qty;
        self.trade_count += 1;
    }
}

#[derive(Default)]
struct LiveSymbolMarketData {
    trades_24h: VecDeque<LiveTradePoint>,
    high_24h: f64,
    low_24h: f64,
    volume_24h: f64,
    kline_1m: Option<LiveBucket>,
    agg_1s: Option<LiveBucket>,
    agg_5s: Option<LiveBucket>,
}

impl LiveSymbolMarketData {
    /// Returns (change, high, low, volume, kline_bucket, agg_1s, agg_5s)
    fn ingest(
        &mut self,
        ts_secs: i64,
        price: f64,
        qty: f64,
    ) -> (f64, f64, f64, f64, LiveBucket, LiveBucket, LiveBucket) {
        self.prune_24h(ts_secs);
        self.push_24h(ts_secs, price, qty);

        let open_24h = self.trades_24h.front().map(|t| t.price).unwrap_or(price);
        let change = if open_24h != 0.0 {
            (price - open_24h) / open_24h * 100.0
        } else {
            0.0
        };

        let kline = update_live_bucket(&mut self.kline_1m, ts_secs - ts_secs % 60, price, qty);
        let agg_1s = update_live_bucket(&mut self.agg_1s, ts_secs, price, qty);
        let agg_5s = update_live_bucket(&mut self.agg_5s, ts_secs - ts_secs % 5, price, qty);

        (
            change,
            self.high_24h,
            self.low_24h,
            self.volume_24h,
            kline,
            agg_1s,
            agg_5s,
        )
    }

    fn prune_24h(&mut self, ts_secs: i64) {
        let cutoff = ts_secs - 86_400;
        let mut recalc = false;
        while let Some(front) = self.trades_24h.front() {
            if front.ts_secs >= cutoff {
                break;
            }
            let old = self.trades_24h.pop_front().unwrap();
            self.volume_24h -= old.qty;
            if old.price == self.high_24h || old.price == self.low_24h {
                recalc = true;
            }
        }
        if recalc {
            self.high_24h = 0.0;
            self.low_24h = 0.0;
            for trade in &self.trades_24h {
                if self.high_24h == 0.0 || trade.price > self.high_24h {
                    self.high_24h = trade.price;
                }
                if self.low_24h == 0.0 || trade.price < self.low_24h {
                    self.low_24h = trade.price;
                }
            }
        }
    }

    fn push_24h(&mut self, ts_secs: i64, price: f64, qty: f64) {
        self.trades_24h.push_back(LiveTradePoint {
            ts_secs,
            price,
            qty,
        });
        if self.high_24h == 0.0 || price > self.high_24h {
            self.high_24h = price;
        }
        if self.low_24h == 0.0 || price < self.low_24h {
            self.low_24h = price;
        }
        self.volume_24h += qty;
    }
}

#[derive(Default)]
struct LiveMarketData {
    by_symbol: HashMap<String, LiveSymbolMarketData>,
}

impl LiveMarketData {
    /// Returns [ticker_sbe, kline_sbe, agg1s_sbe, agg5s_sbe]. All SBE — REST decodes on demand.
    fn ingest(&mut self, symbol: &str, ts_micros: u64, price: f64, qty: f64) -> [Vec<u8>; 4] {
        use lightning_exchange::ws_sbe;
        let ts_secs = (ts_micros / 1_000_000) as i64;
        let state = self.by_symbol.entry(symbol.to_string()).or_default();
        let (change, high, low, volume, kline, agg_1s, agg_5s) = state.ingest(ts_secs, price, qty);
        let interval_1m: u8 = 1;
        let interval_1s: u8 = 0;
        let interval_5s: u8 = 0;
        [
            ws_sbe::encode_ticker(symbol, price, change, high, low, volume),
            ws_sbe::encode_kline(
                symbol,
                interval_1m,
                kline.start as u64,
                kline.open,
                kline.high,
                kline.low,
                kline.close,
                kline.volume,
            ),
            ws_sbe::encode_agg_trade(
                symbol,
                interval_1s,
                agg_1s.start as u64,
                agg_1s.open,
                agg_1s.high,
                agg_1s.low,
                agg_1s.close,
                agg_1s.volume,
                agg_1s.trade_count as u32,
            ),
            ws_sbe::encode_agg_trade(
                symbol,
                interval_5s,
                agg_5s.start as u64,
                agg_5s.open,
                agg_5s.high,
                agg_5s.low,
                agg_5s.close,
                agg_5s.volume,
                agg_5s.trade_count as u32,
            ),
        ]
    }
}

fn update_live_bucket(
    bucket: &mut Option<LiveBucket>,
    start: i64,
    price: f64,
    qty: f64,
) -> LiveBucket {
    match bucket {
        Some(existing) if existing.start == start => {
            existing.update(price, qty);
            *existing
        }
        _ => {
            let next = LiveBucket::new(start, price, qty);
            *bucket = Some(next);
            next
        }
    }
}

// ── DbCmd: fixed-size Copy type for rtrb spin thread → DB worker ─────────────
/// No heap allocation; zero Arc clones on the hot path.

/// One fill carried in a batched settlement cmd. POD/Copy for rtrb.
#[derive(Clone, Copy)]
struct SettleTradeEntry {
    taker_id: i64,
    maker_id: i64,
    taker_uid: i64, // 0 → DB worker resolves via SELECT
    maker_uid: i64,
    price: f64,
    qty: f64,
    side: u8, // 0=buy taker, 1=sell taker
    symbol: [u8; 16],
}

/// One cancel + release pair. Spin thread resolves the asset + amount
/// from the in-memory OrderRuntimeMeta at CANCELED-event time so the DB
/// worker doesn't need a PG SELECT to know what to release. Kept POD/Copy
/// for rtrb.
#[derive(Clone, Copy)]
struct CancelReleaseEntry {
    id: i64,
    user_id: i64,
    /// Resolved release asset ("BTC", "USDT", ...) as null-padded bytes.
    asset: [u8; 16],
    /// Amount to release back from `frozen` into `balance` for that asset.
    release_amount: f64,
}

/// One row in a batched `INSERT INTO orders` cmd. Kept POD/Copy for rtrb.
#[derive(Clone, Copy)]
struct OrderInsertEntry {
    id: i64,
    user_id: i64,
    symbol: [u8; 16],
    side: u8, // 0=buy 1=sell
    order_type: [u8; 16],
    price: f64,
    qty: f64,
    filled: f64,
    status: u8, // DbOrderStatus as u8
    freeze_price: f64,
    do_freeze: bool,
    client_order_id: [u8; 32], // null-padded, max 31 chars
}

#[derive(Clone, Copy)]
enum DbCmd {
    /// INSERT INTO orders … ON CONFLICT DO UPDATE.
    /// do_freeze=true → also call freeze_for_buy/sell after INSERT (GTC ACCEPTED path).
    UpsertOrder {
        id: i64,
        user_id: i64,
        symbol: [u8; 16],
        side: u8, // 0=buy 1=sell
        order_type: [u8; 16],
        price: f64,
        qty: f64,
        filled: f64,
        status: u8, // DbOrderStatus as u8
        freeze_price: f64,
        do_freeze: bool,
        client_order_id: [u8; 32], // null-padded, max 31 chars
    },
    /// Engine-confirmed ACCEPTEDs (batched). Single multi-row INSERT +
    /// grouped UPDATE accounts per (user_id, asset). ~11× faster locally
    /// than per-id sequential INSERT + freeze (examples/bench_upsert_order).
    BatchUpsertOrder {
        entries: [OrderInsertEntry; 64],
        count: u8,
    },
    /// UPDATE orders SET status, filled WHERE id  (REST / subsequent update path).
    UpdateStatus { id: i64, status: u8, filled: f64 },
    /// Engine-confirmed cancels (batched). PR5b: spin thread now passes
    /// pre-computed (user_id, asset, release_amount) per cancel — DB
    /// worker just mutates account_cache and publishes OrderDelete +
    /// AccountSet. No more PG SELECT or UPDATE.
    BatchCancelConfirmed {
        entries: [CancelReleaseEntry; 64],
        count: u8,
    },
    /// Release a pre-engine reservation for an order that never became active.
    ReleaseReservation {
        user_id: i64,
        symbol: [u8; 16],
        side: u8,
        qty: f64,
        freeze_price: f64,
    },
    /// Batched settlement for N fills emitted in the same engine burst.
    /// One txn: multi-row INSERT trades + UPDATE orders FROM (VALUES) +
    /// grouped UPDATE accounts. ~5× faster locally at N=5; ~10× at N=20
    /// (examples/bench_settle_trade).
    BatchSettleTrade {
        entries: [SettleTradeEntry; 64],
        count: u8,
    },
    /// Batched DeleteOrder — collapses N FILLED/REJECTED events from one
    /// engine burst into a single `DELETE … WHERE id = ANY($1)`. ~9×
    /// faster locally (examples/bench_delete_order). Works equally for
    /// count=1. Fixed-array + count to keep DbCmd `Copy`.
    BatchDeleteOrder { ids: [i64; 64], count: u8 },
}

/// Per-order metadata kept in the spin thread's local HashMap so cancel +
/// settle handling never has to SELECT from PG. PR5 expanded this from
/// just `user_id` to the full fund-release shape: when CANCELED arrives,
/// we need to know `freeze_price`, `quantity`, `filled`, `side`, and
/// `symbol` to release the correct asset and amount.
///
/// Fixed-size byte arrays for symbol/side keep this Copy (no String
/// alloc), and let the spin loop work without heap allocation per
/// order_update event.
#[derive(Clone, Copy)]
struct OrderRuntimeMeta {
    user_id: i64,
    freeze_price: f64,
    quantity: f64,
    filled: f64,
    /// 0 = buy, 1 = sell — same encoding as SbeNewOrder.side.
    side: u8,
    symbol: [u8; 16],
    /// Non-zero for forced-liquidation orders. Passed to RiskEngine::on_fill
    /// so the user is settled at this price; spread → insurance fund.
    liq_price_ticks: i64,
    /// Margin reserved by check_and_reserve_margin for this order (atoms).
    /// Stored so the else/CANCELLED path (ws_meta=None) can release it.
    initial_margin_atoms: i64,
}

#[allow(clippy::too_many_arguments)]
fn remember_runtime_order(
    cache: &DashMap<u64, OrderRuntimeMeta>,
    order_id: u64,
    user_id: i64,
    freeze_price: f64,
    quantity: f64,
    side: u8,
    symbol: [u8; 16],
    initial_margin_atoms: i64,
) {
    cache.insert(
        order_id,
        OrderRuntimeMeta {
            user_id,
            freeze_price,
            quantity,
            filled: 0.0,
            side,
            symbol,
            liq_price_ticks: 0,
            initial_margin_atoms,
        },
    );
}

fn remap_runtime_order_id(cache: &DashMap<u64, OrderRuntimeMeta>, from: u64, to: u64) {
    if from != to {
        if let Some((_, meta)) = cache.remove(&from) {
            cache.insert(to, meta);
        }
    }
}

fn runtime_user_id(cache: &DashMap<u64, OrderRuntimeMeta>, order_id: u64) -> i64 {
    cache.get(&order_id).map(|m| m.user_id).unwrap_or(0)
}

/// Lookup full meta (used by cancel/fill release paths in PR5b/d).
fn runtime_meta(cache: &DashMap<u64, OrderRuntimeMeta>, order_id: u64) -> Option<OrderRuntimeMeta> {
    cache.get(&order_id).map(|r| *r)
}

/// Update the filled accumulator after a partial-fill event so subsequent
/// cancel-release uses the right remaining quantity.
fn runtime_bump_filled(cache: &DashMap<u64, OrderRuntimeMeta>, order_id: u64, filled_qty: f64) {
    if let Some(mut meta) = cache.get_mut(&order_id) {
        meta.filled = filled_qty;
    }
}

fn remove_runtime_order(cache: &DashMap<u64, OrderRuntimeMeta>, order_id: u64, client_id: u64) {
    cache.remove(&order_id);
    if order_id != client_id {
        cache.remove(&client_id);
    }
}

fn try_freeze_cache(cache: &AccountCache, user_id: i64, asset: &str, amount: f64) -> bool {
    if amount <= 0.0 {
        return true;
    }
    let Some(mut entry) = cache.get_mut(&user_id) else {
        return false;
    };
    let Some(kv) = entry.get_mut(asset) else {
        return false;
    };
    let Ok(amount_atoms) = AmountAtoms::from_f64_round(amount) else {
        return false;
    };
    kv.try_freeze_atoms(amount_atoms.atoms())
}

fn release_cache_frozen(cache: &AccountCache, user_id: i64, asset: &str, amount: f64) {
    if amount <= 0.0 {
        return;
    }
    if let Some(mut entry) = cache.get_mut(&user_id) {
        if let Some(kv) = entry.get_mut(asset) {
            if let Ok(amount_atoms) = AmountAtoms::from_f64_round(amount) {
                kv.release_atoms(amount_atoms.atoms());
            }
        }
    }
}

fn order_meta_from_forward(
    meta: CounterForwardOrderMeta,
) -> lightning_exchange::transport::OrderMeta {
    let symbol = meta.symbol;
    let side = meta.side;
    let order_type = meta.order_type;
    let price = meta.price;
    let qty = meta.qty;
    let freeze_price = meta.freeze_price;
    let initial_margin_atoms = meta.initial_margin_atoms;
    lightning_exchange::transport::OrderMeta {
        user_id: meta.user_id,
        symbol,
        side,
        order_type,
        price: if price > 0.0 { Some(price) } else { None },
        qty,
        client_order_id: meta.client_order_id_string(),
        freeze_price,
        initial_margin_atoms,
        liq_price_ticks: 0,
    }
}

fn log_counter_forward_publish_failure(counter: &AtomicU64, path: &str) {
    let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
    if n == 1 || n % 10_000 == 0 {
        tracing::warn!("counter-forward publish failed: path={path} total={n}");
    }
}

fn new_counter_forward_publisher_with_retry(
    client: Arc<AeronClient>,
    channel: &str,
    stream: i32,
    label: &str,
) -> CounterForwardPublisher {
    let mut attempts = 0u32;
    loop {
        match CounterForwardPublisher::new(client.clone(), channel, stream) {
            Ok(pub_) => return pub_,
            Err(e) => {
                attempts += 1;
                if attempts == 1 || attempts % 100 == 0 {
                    tracing::warn!(
                        "counter-forward publisher registration retry: label={} stream={} attempts={} error={}",
                        label,
                        stream,
                        attempts,
                        e
                    );
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

fn counter_forward_debug_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("COUNTER_FORWARD_DEBUG")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

macro_rules! counter_forward_debug {
    ($($arg:tt)*) => {
        if counter_forward_debug_enabled() {
            eprintln!($($arg)*);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym(s: &str) -> [u8; 16] {
        let mut b = [0u8; 16];
        let n = s.as_bytes().len().min(16);
        b[..n].copy_from_slice(&s.as_bytes()[..n]);
        b
    }

    #[test]
    fn runtime_meta_survives_until_terminal_remove() {
        let cache = DashMap::new();
        remember_runtime_order(&cache, 10, 7, 73000.0, 0.5, 0, sym("BTC_USDT"), 0);

        assert_eq!(runtime_user_id(&cache, 10), 7);
        assert_eq!(runtime_user_id(&cache, 11), 0);
        let m = runtime_meta(&cache, 10).expect("meta present");
        assert_eq!(m.freeze_price, 73000.0);
        assert_eq!(m.quantity, 0.5);
        assert_eq!(m.side, 0);

        remove_runtime_order(&cache, 10, 10);
        assert_eq!(runtime_user_id(&cache, 10), 0);
        assert!(runtime_meta(&cache, 10).is_none());
    }

    #[test]
    fn runtime_meta_remaps_client_id_to_engine_order_id() {
        let cache = DashMap::new();
        remember_runtime_order(&cache, 10, 7, 100.0, 1.0, 1, sym("BTC_USDT"), 0);

        remap_runtime_order_id(&cache, 10, 99);

        assert_eq!(runtime_user_id(&cache, 10), 0);
        assert_eq!(runtime_user_id(&cache, 99), 7);
        let m = runtime_meta(&cache, 99).expect("meta present after remap");
        assert_eq!(m.freeze_price, 100.0);
        assert_eq!(m.side, 1);

        remove_runtime_order(&cache, 99, 10);
        assert_eq!(runtime_user_id(&cache, 99), 0);
    }

    #[test]
    fn runtime_bump_filled_updates_accumulator() {
        let cache = DashMap::new();
        remember_runtime_order(&cache, 5, 1, 100.0, 2.0, 0, sym("X_Y"), 0);
        assert_eq!(runtime_meta(&cache, 5).unwrap().filled, 0.0);
        runtime_bump_filled(&cache, 5, 0.5);
        assert_eq!(runtime_meta(&cache, 5).unwrap().filled, 0.5);
        runtime_bump_filled(&cache, 5, 1.5);
        assert_eq!(runtime_meta(&cache, 5).unwrap().filled, 1.5);
    }
}

// ── process_db_cmd: now sync, called directly from the recv-spin thread ──────
/// Enqueue PersistFrames for the dedicated persist-send thread. Never spin on
/// Aeron from recv-spin; dropping here preserves accepted-update latency under
/// persistence overload, and Redis/PG can be reconciled from later snapshots.
fn publish_frame(
    tx: &std::sync::Arc<crossbeam_queue::ArrayQueue<PersistFrame>>,
    frame: &PersistFrame,
) {
    static PERSIST_DROPPED: AtomicU64 = AtomicU64::new(0);
    if tx.push(*frame).is_err() {
        let n = PERSIST_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
        if n % 100_000 == 0 {
            tracing::warn!("persist queue full — dropped {n} frames");
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_matching_order_event(
    tx: &std::sync::Arc<crossbeam_queue::ArrayQueue<PersistFrame>>,
    sequence: u64,
    response_stream_id: i32,
    kind: u8,
    order_id: u64,
    client_order_id: u64,
    participant_id: u64,
    fill_price: f64,
    fill_qty: f64,
    remaining_qty: f64,
    timestamp_ns: u64,
    meta: &OrderRuntimeMeta,
) {
    let symbol = unpack_str16(&meta.symbol).unwrap_or("BTC_USDT");
    let rules = lightning_exchange::desk::symbol_rules::SymbolRules::for_symbol(symbol);
    let event_kind = match kind {
        lightning_exchange::transport::order_update_kind::ACCEPTED => matching_event_kind::ACCEPTED,
        lightning_exchange::transport::order_update_kind::FILLED => matching_event_kind::FILLED,
        lightning_exchange::transport::order_update_kind::PARTIAL_FILL => {
            matching_event_kind::PARTIAL_FILL
        }
        lightning_exchange::transport::order_update_kind::CANCELLED => matching_event_kind::CANCELLED,
        lightning_exchange::transport::order_update_kind::REJECTED => matching_event_kind::REJECTED,
        _ => return,
    };
    let event_order_id = if order_id == 0 {
        client_order_id
    } else {
        order_id
    };
    let price = if fill_price > 0.0 {
        fill_price
    } else {
        meta.freeze_price
    };
    let qty = if fill_qty > 0.0 { fill_qty } else { meta.quantity };
    let remaining = if remaining_qty > 0.0 {
        remaining_qty
    } else if event_kind == matching_event_kind::ACCEPTED {
        meta.quantity
    } else {
        0.0
    };
    let price_ticks = if price > 0.0 {
        rules
            .price_to_ticks(price)
            .unwrap_or_else(|_| (price / rules.price_tick).round() as i64)
    } else {
        0
    };
    let quantity_lots = rules
        .quantity_to_lots(qty)
        .unwrap_or_else(|_| (qty / rules.quantity_step).round() as i64);
    let remaining_lots = rules
        .quantity_to_lots(remaining)
        .unwrap_or_else(|_| (remaining / rules.quantity_step).round() as i64);
    publish_frame(
        tx,
        &PersistFrame::matching_event(MatchingEventPayload {
            sequence,
            response_stream_id,
            event_kind,
            _pad: [0; 3],
            order_id: event_order_id as i64,
            client_order_id: client_order_id as i64,
            participant_id: participant_id as i64,
            counterparty_order_id: 0,
            symbol: pack_str(symbol),
            price_ticks,
            quantity_lots,
            remaining_lots,
            ts_ns: timestamp_ns,
        }),
    );
}

fn account_set_payload(
    user_id: i64,
    asset: &str,
    balance: f64,
    frozen: f64,
) -> anyhow::Result<AccountSetPayload> {
    let balance_atoms = lightning_exchange::money::AmountAtoms::from_f64_round(balance)?.atoms();
    let frozen_atoms = lightning_exchange::money::AmountAtoms::from_f64_round(frozen)?.atoms();
    Ok(AccountSetPayload {
        user_id,
        asset: pack_str(asset),
        balance,
        frozen,
        balance_atoms,
        frozen_atoms,
    })
}

#[allow(clippy::too_many_arguments)]
/// Now pure-sync — every PG SQL call was already removed by PR5; what
/// remains is publish_frame (Aeron offer ~1µs), DashMap updates on
/// account_cache + vwap_cache, and user_tx.try_send. Callable directly
/// from the recv-spin thread; no need for a separate db-worker thread
/// or tokio::spawn wrapping. Saves one thread + one rtrb hop per command.
/// S5.3/S5.4 — convert one fired (or recovery-orphaned) trigger into a
/// real order. The PG status flip is the exactly-once anchor: only the
/// rows_affected==1 winner proceeds. Margin is reserved BEFORE the flip;
/// a reserve failure cancels the trigger with reason instead of
/// injecting a doomed order (S5.4).
#[allow(clippy::too_many_arguments)]
async fn inject_trigger_order(
    pool: &std::sync::Arc<sqlx::PgPool>,
    engine: &std::sync::Arc<lightning_exchange::desk::risk::RiskEngine>,
    cmd_tx: &Option<std::sync::Arc<crossbeam_queue::ArrayQueue<lightning_exchange::transport::AeronCmd>>>,
    pending_meta: &std::sync::Arc<DashMap<u64, lightning_exchange::transport::OrderMeta>>,
    runtime_meta_cache: &DashMap<u64, OrderRuntimeMeta>,
    response_stream_id: i32,
    trigger_id: i64,
    order_id: i64,
    user_id: i64,
    symbol: &str,
    side: u8,
    is_market: bool,
    price_ticks: Option<i64>,
    qty_lots: i64,
    already_marked: bool,
) {
    use lightning_exchange::sbe::NewOrderRequest as SbeNewOrder;
    use lightning_exchange::transport::{AeronCmd, OrderMeta, pack_str16};
    let rules = lightning_exchange::desk::symbol_rules::SymbolRules::for_symbol(symbol);
    let sym16 = lightning_exchange::transport::persist_event::pack_str(symbol);
    let mark = engine.mark_price_ticks(&sym16).unwrap_or(0);
    // Market triggers convert to an aggressive IOC limit 5% through the
    // mark (same shape as liquidation orders — fills at market, the cap
    // only guards against an empty far side).
    let exec_ticks = match (is_market, price_ticks) {
        (false, Some(p)) => p,
        _ => {
            if mark <= 0 {
                tracing::warn!("trigger {trigger_id}: no mark for market conversion — cancelled");
                let _ = sqlx::query(
                    "UPDATE trigger_orders SET status='cancelled', cancel_reason='no mark'
                      WHERE id=$1 AND status IN ('pending','triggered')",
                )
                .bind(trigger_id)
                .execute(pool.as_ref())
                .await;
                return;
            }
            if side == 0 { mark + mark / 20 } else { mark - mark / 20 }
        }
    };
    if exec_ticks <= 0 {
        return;
    }

    // S5.4: same margin gate as direct order entry. Lazy-init the risk
    // account from the ledger when this user has never been seen by the
    // margin engine in this process lifetime (e.g. registered while a
    // previous desk incarnation was running).
    let margin =
        lightning_exchange::desk::trigger::firing_margin_atoms(
            exec_ticks, qty_lots, rules.notional_scale, rules.default_leverage,
        );
    if !engine.accounts.contains_key(&user_id) {
        let usdt: Option<i64> = sqlx::query_scalar(
            "SELECT balance_atoms FROM accounts WHERE user_id = $1 AND asset = 'USDT'",
        )
        .bind(user_id)
        .fetch_optional(pool.as_ref())
        .await
        .unwrap_or(None);
        engine.initialize_account(user_id, usdt.unwrap_or(0));
    }
    if engine.check_and_reserve_margin(user_id, margin).is_err() {
        tracing::warn!(
            "trigger {trigger_id}: margin reserve {margin} failed for user {user_id} — cancelled"
        );
        let _ = sqlx::query(
            "UPDATE trigger_orders SET status='cancelled', cancel_reason='insufficient margin'
              WHERE id=$1 AND status IN ('pending','triggered')",
        )
        .bind(trigger_id)
        .execute(pool.as_ref())
        .await;
        return;
    }

    // Exactly-once anchor (skipped when recovery already holds the flip).
    if !already_marked {
        let flipped = sqlx::query(
            "UPDATE trigger_orders
                SET status='triggered', triggered_order_id=$2, triggered_at=NOW()
              WHERE id=$1 AND status='pending'",
        )
        .bind(trigger_id)
        .bind(order_id)
        .execute(pool.as_ref())
        .await
        .map(|r| r.rows_affected() == 1)
        .unwrap_or(false);
        if !flipped {
            // Lost the race to a user cancel — give the margin back.
            engine.release_order_margin(user_id, margin);
            return;
        }
    }

    // Durable order row BEFORE the publish (the recovery footprint).
    let price_f = exec_ticks as f64 * rules.price_tick;
    let qty_f = rules.lots_to_quantity(qty_lots);
    let ot = if is_market { "trigger-market" } else { "trigger-limit" };
    if let Err(e) = sqlx::query(
        "INSERT INTO orders (id, user_id, symbol, side, order_type,
                             price_atoms, quantity_atoms, filled_atoms, status, freeze_price_atoms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 'TRADING', 0)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(order_id)
    .bind(user_id)
    .bind(symbol)
    .bind(if side == 0 { "buy" } else { "sell" })
    .bind(ot)
    .bind(exec_ticks * rules.price_tick_atoms)
    .bind(qty_lots * rules.quantity_step_atoms)
    .execute(pool.as_ref())
    .await
    {
        tracing::error!("trigger {trigger_id}: orders insert failed: {e}");
        engine.release_order_margin(user_id, margin);
        return;
    }

    pending_meta.insert(
        order_id as u64,
        OrderMeta {
            user_id,
            symbol: sym16,
            side,
            order_type: pack_str16(ot),
            price: Some(price_f),
            qty: qty_f,
            client_order_id: format!("trig-{trigger_id}"),
            freeze_price: 0.0,
            initial_margin_atoms: margin,
            liq_price_ticks: 0,
        },
    );
    // Runtime meta BEFORE the publish: the engine emits the TRADE frame
    // before the ACCEPTED update, so the trade consumer resolves
    // taker_uid through this cache — without this entry an injected
    // order's first fill settles with uid 0 and the trades row is
    // silently dropped (caught live by the S5 drill).
    remember_runtime_order(
        runtime_meta_cache,
        order_id as u64,
        user_id,
        0.0,
        qty_f,
        side,
        sym16,
        margin,
    );
    let req = SbeNewOrder {
        client_order_id: order_id as u64,
        participant_id: user_id as u64,
        price_ticks: exec_ticks,
        quantity_lots: qty_lots,
        side,
        time_in_force: if is_market { 1 } else { 0 }, // IOC for market
        response_stream_id,
        reduce_only: 0,
        _pad: [0; 9],
        symbol: sym16,
    };
    let Some(cmd_tx) = cmd_tx else {
        tracing::error!("trigger {trigger_id}: no engine command queue — order stranded");
        return;
    };
    if cmd_tx.push(AeronCmd::NewOrder(req)).is_err() {
        tracing::error!("trigger {trigger_id}: engine queue full — recovery will re-inject");
    } else {
        tracing::info!(
            "trigger {trigger_id} FIRED → order {order_id} ({symbol} {} {} @ {exec_ticks})",
            if side == 0 { "buy" } else { "sell" },
            qty_lots
        );
    }
}

fn process_db_cmd(
    cmd: DbCmd,
    account_cache: &lightning_exchange::api::AccountCache,
    user_tx: &lightning_exchange::api::UserTxRegistry,
    persist_pub: &std::sync::Arc<crossbeam_queue::ArrayQueue<PersistFrame>>,
    vwap_cache: &lightning_exchange::api::VwapCache,
    risk_engine: &std::sync::Arc<lightning_exchange::desk::risk::RiskEngine>,
    exchange_config_pdc: &std::sync::Arc<lightning_exchange::desk::exchange_config::ExchangeConfig>,
) {
    match cmd {
        DbCmd::UpsertOrder {
            id,
            user_id: _user_id,
            symbol,
            side,
            order_type,
            price,
            qty,
            filled,
            status,
            freeze_price,
            do_freeze: _do_freeze,
            client_order_id,
        } => {
            // PR5c: this variant is now only pushed by the PARTIAL_FILL
            // path in the spin thread (do_freeze=false). Publish an
            // OrderFillUpdate frame so Redis HASH sees the new
            // filled/status, and let pg-writer apply the same update
            // asynchronously to PG. No DB worker SQL here.
            let _ = price;
            let _ = qty;
            let _ = freeze_price;
            let _ = symbol;
            let _ = order_type;
            let _ = client_order_id;
            let _ = side;
            publish_frame(
                &persist_pub,
                &PersistFrame::order_fill_update(OrderFillUpdatePayload {
                    id,
                    filled,
                    status,
                    _pad: [0; 7],
                }),
            );
        }

        DbCmd::BatchUpsertOrder { entries, count } => {
            // PR5c: no more PG. Cache was already updated at freeze time
            // by ws_handler::try_freeze_cache / REST handler's same path;
            // the WS-handler also already pushed balance_update. Here we
            // just publish OrderUpsert (so pg-writer + redis-writer mirror
            // the new rows) and AccountSet (with current cache values)
            // for completeness.
            let count = count as usize;
            if count == 0 {
                return;
            }
            let entries = &entries[..count];

            // For AccountSet: dedupe by (user_id, asset). We read from cache,
            // so we don't accumulate amounts here — just one set per pair.
            let mut accounts_to_emit: HashMap<(i64, String), ()> = HashMap::new();
            for e in entries {
                if !e.do_freeze {
                    continue;
                }
                let sym_end = e.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                let sym = std::str::from_utf8(&e.symbol[..sym_end]).unwrap_or("BTC_USDT");
                let parts: Vec<&str> = sym.splitn(2, '_').collect();
                let base = parts.first().copied().unwrap_or("BTC");
                let quote = parts.last().copied().unwrap_or("USDT");
                let asset = if e.side == 0 {
                    quote.to_string()
                } else {
                    base.to_string()
                };
                accounts_to_emit.insert((e.user_id, asset), ());
            }
            for ((uid, asset), _) in accounts_to_emit {
                let snapshot: Option<AccountBalance> = account_cache
                    .get(&uid)
                    .and_then(|entry| entry.get(asset.as_str()).copied());
                if let Some(snapshot) = snapshot {
                    let bal = snapshot.balance();
                    let frz = snapshot.frozen();
                    match account_set_payload(uid, &asset, bal, frz) {
                        Ok(payload) => {
                            publish_frame(&persist_pub, &PersistFrame::account_set(payload));
                        }
                        Err(e) => tracing::warn!("skip AccountSet persist frame: {e}"),
                    }
                    // No WS push here — try_freeze_cache already sent it.
                    let _ = user_tx.get(uid);
                }
            }

            // Publish OrderUpsert per row so redis-writer + pg-writer mirror
            // the new rows.
            for e in entries {
                publish_frame(
                    &persist_pub,
                    &PersistFrame::order_upsert(OrderUpsertPayload {
                        id: e.id,
                        user_id: e.user_id,
                        symbol: e.symbol,
                        side: e.side,
                        status: e.status,
                        _pad: [0; 6],
                        order_type: e.order_type,
                        price: e.price,
                        qty: e.qty,
                        filled: e.filled,
                        freeze_price: e.freeze_price,
                        client_order_id: e.client_order_id,
                        created_at_ms: chrono::Utc::now().timestamp_millis(),
                    }),
                );
            }
        }

        DbCmd::UpdateStatus { id, status, filled } => {
            // PR5e: publish only. pg-writer applies the same status/filled
            // change to PG asynchronously via OrderFillUpdate.
            publish_frame(
                &persist_pub,
                &PersistFrame::order_fill_update(OrderFillUpdatePayload {
                    id,
                    filled,
                    status,
                    _pad: [0; 7],
                }),
            );
        }

        DbCmd::BatchCancelConfirmed { entries, count } => {
            // PR5b: spin thread already computed (user_id, asset,
            // release_amount) per cancel using OrderRuntimeMeta. DB
            // worker is now pure cache mutate + publish — no PG round
            // trips. pg-writer (when running) picks up the same
            // AccountSet + OrderDelete frames from the persist stream
            // and applies them to PG asynchronously.
            let count = count as usize;
            if count == 0 {
                return;
            }
            let entries = &entries[..count];

            // Group releases by (user_id, asset) to coalesce the cache
            // mutate + balance_update WS push. Typical MM cancel cycle
            // touches two groups (USDT for bids, base for asks).
            let mut releases: HashMap<(i64, String), f64> = HashMap::new();
            for e in entries {
                if e.release_amount <= 0.0 || e.user_id == 0 {
                    continue;
                }
                let asset_end = e.asset.iter().position(|&b| b == 0).unwrap_or(16);
                let asset = match std::str::from_utf8(&e.asset[..asset_end]) {
                    Ok(s) if !s.is_empty() => s.to_owned(),
                    _ => continue,
                };
                *releases.entry((e.user_id, asset)).or_insert(0.0) += e.release_amount;
            }
            for ((uid, asset), amount) in releases {
                // Atomic in-memory release: frozen -= amount (clamped to 0).
                // Balance unchanged — for a cancel, the funds were never
                // actually debited, only frozen; releasing just decrements
                // the frozen counter back into available.
                let new_vals = account_cache.get_mut(&uid).and_then(|mut entry| {
                    let kv = entry.get_mut(&asset)?;
                    let amount_atoms = AmountAtoms::from_f64_round(amount).ok()?;
                    kv.release_atoms(amount_atoms.atoms());
                    Some(*kv)
                });
                if let Some(snapshot) = new_vals {
                    let bal = snapshot.balance();
                    let frz = snapshot.frozen();
                    match account_set_payload(uid, &asset, bal, frz) {
                        Ok(payload) => {
                            publish_frame(&persist_pub, &PersistFrame::account_set(payload));
                        }
                        Err(e) => tracing::warn!("skip AccountSet persist frame: {e}"),
                    }
                    if let Some(tx) = user_tx.get(uid) {
                        let _ = tx.try_send((
                            lightning_exchange::ws_sbe::encode_balance_update(
                                &asset,
                                bal,
                                bal - frz,
                                frz,
                            ),
                            0,
                        ));
                    }
                }
            }
            // Publish OrderDelete for every requested id (idempotent — see
            // commit 556d523 for the audit of why we publish ALL ids, not
            // only those that existed in PG).
            for e in entries {
                publish_frame(
                    &persist_pub,
                    &PersistFrame::order_delete(OrderDeletePayload { id: e.id }),
                );
            }
        }

        DbCmd::ReleaseReservation {
            user_id,
            symbol,
            side,
            qty,
            freeze_price,
        } => {
            // PR5e: cache-only release. Was a PG UPDATE accounts via
            // repo.release_frozen. Same correctness — cache is source of
            // truth; pg-writer mirrors the AccountSet to PG asynchronously.
            let sym_end = symbol.iter().position(|&b| b == 0).unwrap_or(16);
            let sym_str = std::str::from_utf8(&symbol[..sym_end]).unwrap_or("BTC_USDT");
            let parts: Vec<&str> = sym_str.splitn(2, '_').collect();
            let base = parts.first().copied().unwrap_or("BTC");
            let quote = parts.last().copied().unwrap_or("USDT");
            let (asset, amount) = if side == 0 {
                (quote.to_string(), freeze_price * qty)
            } else {
                (base.to_string(), qty)
            };
            if amount <= 0.0 {
                return;
            }
            let new_vals = account_cache.get_mut(&user_id).and_then(|mut entry| {
                let kv = entry.get_mut(&asset)?;
                let amount_atoms = AmountAtoms::from_f64_round(amount).ok()?;
                kv.release_atoms(amount_atoms.atoms());
                Some(*kv)
            });
            if let Some(snapshot) = new_vals {
                let bal = snapshot.balance();
                let frz = snapshot.frozen();
                match account_set_payload(user_id, &asset, bal, frz) {
                    Ok(payload) => {
                        publish_frame(&persist_pub, &PersistFrame::account_set(payload));
                    }
                    Err(e) => tracing::warn!("skip AccountSet persist frame: {e}"),
                }
                if let Some(tx) = user_tx.get(user_id) {
                    let _ = tx.try_send((
                        lightning_exchange::ws_sbe::encode_balance_update(
                            &asset,
                            bal,
                            bal - frz,
                            frz,
                        ),
                        0,
                    ));
                }
            }
        }

        DbCmd::BatchDeleteOrder { ids, count } => {
            // PR5e: pure publish. PG DELETE is now pg-writer's job.
            let count = count as usize;
            if count == 0 {
                return;
            }
            let ids_vec = &ids[..count];
            for &id in ids_vec {
                publish_frame(
                    &persist_pub,
                    &PersistFrame::order_delete(OrderDeletePayload { id }),
                );
            }
        }

        DbCmd::BatchSettleTrade { entries, count } => {
            // PR5d: in-memory settlement, no PG. Spin thread already
            // S1.6: MAKER margin updates run over ALL maker-resolvable
            // entries, NOT the both-sides filter below. The engine emits
            // OrderUpdates only to the TAKER (makers learn of fills solely
            // through this trade path), and the taker's FILLED can race
            // ahead in the spin thread and GC the taker's runtime meta
            // before this thread polls the trade — taker_uid=0 must not
            // cost the maker its position update (caught by the
            // chaos_position_persist drill, close-leg flake). Contract:
            // taker margin → spin thread Phase 2; maker margin → here.
            for e in entries.iter().take(count as usize) {
                if e.maker_uid == 0 {
                    continue;
                }
                let sym_end = e.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                let sym_str = std::str::from_utf8(&e.symbol[..sym_end]).unwrap_or("BTC_USDT");
                let rules =
                    lightning_exchange::desk::symbol_rules::SymbolRules::for_symbol(sym_str);
                let fp_ticks = (e.price / rules.price_tick).round() as i64;
                let fq_lots = rules.quantity_to_lots(e.qty).unwrap_or(0);
                if fq_lots <= 0 {
                    continue;
                }
                let maker_side = 1 - e.side; // opposite of the taker
                let notional = lightning_exchange::desk::risk::calc::calc_notional_atoms(
                    fp_ticks,
                    fq_lots,
                    rules.notional_scale,
                );
                let fill_margin = lightning_exchange::desk::risk::calc::calc_initial_margin_atoms(
                    notional,
                    rules.default_leverage,
                );
                risk_engine.on_fill(
                    e.maker_uid,
                    e.symbol,
                    maker_side,
                    fp_ticks,
                    fq_lots,
                    fill_margin,
                    rules.notional_scale,
                    rules.default_leverage,
                    rules.maintenance_rate_bps,
                    0, // makers are never the liquidation order
                );
                // S7: maker fee on this fill's notional → insurance fund.
                let (maker_bps, _) = exchange_config_pdc.fee_bps(sym_str);
                let fill_notional = lightning_exchange::desk::risk::calc::calc_notional_atoms(
                    fp_ticks, fq_lots, rules.notional_scale,
                );
                risk_engine.charge_fee(
                    e.maker_uid,
                    lightning_exchange::desk::risk::calc::fee_atoms(fill_notional, maker_bps),
                );
                for frame in lightning_exchange::desk::risk_persist::margin_state_frames(
                    risk_engine,
                    e.maker_uid,
                    &e.symbol,
                ) {
                    publish_frame(persist_pub, &frame);
                }
            }

            // resolved taker/maker uids; we just apply per-(user,asset)
            // net deltas to the account_cache atomically, then publish
            // TradeInsert + OrderDelete/OrderFillUpdate + AccountSet for
            // pg-writer to mirror.
            let count = count as usize;
            if count == 0 {
                return;
            }
            let entries = &entries[..count];

            // Skip fills with missing uids — same defensive behaviour as
            // the old PG path which fell back to a SELECT. PR5a's preload
            // means this should never trigger in steady state.
            let resolved: Vec<&SettleTradeEntry> = entries
                .iter()
                .filter(|e| e.taker_uid != 0 && e.maker_uid != 0)
                .collect();
            // T3 — TradeInsert per fill BEFORE the uid-resolution gate: a
            // trade history row needs only the two ORDER ids (always
            // present), not the participant uids. The recv-public trade
            // consumer often can't resolve uids (the taker's runtime meta
            // lives in the recv-PRIVATE thread's cache), which left
            // `resolved` empty and returned here — silently dropping every
            // trade row while positions still landed via the maker loop
            // above. Emit here, unconditionally on order ids.
            {
                let ts_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                for e in entries.iter() {
                    if e.taker_id == 0 || e.maker_id == 0 {
                        continue;
                    }
                    let sym_end = e.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                    let symbol = std::str::from_utf8(&e.symbol[..sym_end]).unwrap_or("BTC_USDT");
                    publish_frame(
                        &persist_pub,
                        &PersistFrame::trade_insert(TradeInsertPayload {
                            buy_order_id: if e.side == 0 { e.taker_id } else { e.maker_id },
                            sell_order_id: if e.side == 0 { e.maker_id } else { e.taker_id },
                            symbol: pack_str(symbol),
                            price: e.price,
                            qty: e.qty,
                            ts_ms,
                        }),
                    );
                }
            }
            if resolved.is_empty() {
                return;
            }

            // NOTE: trade WS broadcast + last_trade_price update happen on
            // the SPIN thread, immediately when each trade pops out of
            // trade_sub.poll(). Doing it here would duplicate every event
            // and add ms-scale cross-thread latency.
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);

            // Net (balance_delta, frozen_release) per (user_id, asset).
            //   buyer  quote: -cost, release cost
            //   buyer  base : +qty
            //   seller base : -qty,  release qty
            //   seller quote: +cost
            #[derive(Default, Clone, Copy)]
            struct Delta {
                balance_atoms: i64,
                frozen_release_atoms: i64,
            }
            let mut deltas: HashMap<(i64, String), Delta> = HashMap::new();
            for r in &resolved {
                let sym_end = r.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                let symbol = std::str::from_utf8(&r.symbol[..sym_end]).unwrap_or("BTC_USDT");
                let (base, quote) = match symbol.split_once('_') {
                    Some(parts) => parts,
                    None => continue,
                };
                let cost = r.price * r.qty;
                let Ok(cost_atoms) = AmountAtoms::from_f64_round(cost).map(|v| v.atoms()) else {
                    continue;
                };
                let Ok(qty_atoms) = AmountAtoms::from_f64_round(r.qty).map(|v| v.atoms()) else {
                    continue;
                };
                let (buyer_uid, seller_uid) = if r.side == 0 {
                    (r.taker_uid, r.maker_uid)
                } else {
                    (r.maker_uid, r.taker_uid)
                };

                let e = deltas.entry((buyer_uid, quote.to_string())).or_default();
                e.balance_atoms -= cost_atoms;
                e.frozen_release_atoms += cost_atoms;

                let e = deltas.entry((buyer_uid, base.to_string())).or_default();
                e.balance_atoms += qty_atoms;

                let e = deltas.entry((seller_uid, base.to_string())).or_default();
                e.balance_atoms -= qty_atoms;
                e.frozen_release_atoms += qty_atoms;

                let e = deltas.entry((seller_uid, quote.to_string())).or_default();
                e.balance_atoms += cost_atoms;

                // VWAP cache for /api/positions: running weighted sum of
                // BUY fills per (user_id, base_asset). Replaces the 90ms
                // PG aggregate previously done per call. Sell fills do
                // not contribute to entry_price (no avg-cost change on
                // exit — already accounted for as realized PnL).
                let mut vw = vwap_cache.entry((buyer_uid, base.to_string())).or_default();
                vw.weighted_sum += cost;
                vw.qty_sum += r.qty;
            }

            // Apply each delta to the in-memory cache atomically. For assets
            // the user has never held, insert a fresh entry. balance >= 0
            // invariant is enforced via clamp (settlement should never push
            // it negative if freezes were correct upstream, but we clamp
            // anyway for defence).
            let mut updated_accounts: Vec<(i64, String, f64, f64)> =
                Vec::with_capacity(deltas.len());
            for ((uid, asset), d) in deltas {
                let mut entry = account_cache.entry(uid).or_insert_with(HashMap::new);
                let kv = entry
                    .entry(asset.clone())
                    .or_insert_with(AccountBalance::default);
                kv.apply_atoms_delta(d.balance_atoms, d.frozen_release_atoms);
                let bal = kv.balance();
                let frz = kv.frozen();
                drop(entry);
                updated_accounts.push((uid, asset, bal, frz));
            }

            // Maker order_update WS + OrderFillUpdate / OrderDelete publish.
            // Spin thread also has maker meta in runtime cache — but here we
            // operate without it because the spin thread will see its own
            // OrderUpdate(kind=FILLED|PARTIAL_FILL) for the maker too and
            // handle bookkeeping there.
            //
            // For now we just publish OrderFillUpdate per maker fill so
            // Redis HASH filled/status converges; if it's a full fill the
            // OrderDelete from the spin thread's else-branch (BatchDeleteOrder)
            // will eventually arrive.
            for r in &resolved {
                if let Some(tx) = user_tx.get(r.maker_uid) {
                    let _ = tx.try_send((
                        lightning_exchange::ws_sbe::encode_order_update(
                            r.maker_id as u64,
                            0,
                            lightning_exchange::ws_sbe::WS_STATUS_PARTIAL_FILL,
                            r.qty,
                            r.price,
                            ts,
                        ),
                        0,
                    ));
                }
                // Publish a TRADING-status fill update for the maker. If the
                // order fully filled, the spin thread will subsequently
                // emit BatchDeleteOrder → OrderDelete; redis_store will
                // process it and remove the HASH.
                publish_frame(
                    &persist_pub,
                    &PersistFrame::order_fill_update(OrderFillUpdatePayload {
                        id: r.maker_id,
                        filled: r.qty, // delta (rebroadcast each time)
                        status: DbOrderStatus::Trading.as_u8(),
                        _pad: [0; 7],
                    }),
                );
            }

            // Publish AccountSet + WS balance_update per updated account.
            for (uid, asset, bal, frz) in updated_accounts {
                match account_set_payload(uid, &asset, bal, frz) {
                    Ok(payload) => {
                        publish_frame(&persist_pub, &PersistFrame::account_set(payload));
                    }
                    Err(e) => tracing::warn!("skip AccountSet persist frame: {e}"),
                }
                if let Some(tx) = user_tx.get(uid) {
                    let _ = tx.try_send((
                        lightning_exchange::ws_sbe::encode_balance_update(
                            &asset,
                            bal,
                            bal - frz,
                            frz,
                        ),
                        0,
                    ));
                }
            }

        }
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    lightning_exchange::util::install_panic_hook();
    raise_nofile_limit();
    // A 40K run usually starts 4 desk-server processes on one host. Defaulting
    // each process to 8 Tokio workers creates 32 scheduler threads before the
    // Aeron spin threads even start, which preempts the matching thread. Keep
    // the default small; override with TOKIO_WORKER_THREADS when a desk owns
    // more cores.
    let worker_threads: usize = std::env::var("TOKIO_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    // event_interval: how many task polls between I/O driver checks (default 61).
    // Lower values reduce WS frame wake latency at the cost of more epoll syscalls.
    // 7 is a practical balance; set to 1 for minimum latency on a dedicated server.
    let event_interval: u32 = std::env::var("TOKIO_EVENT_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .event_interval(event_interval)
        .enable_all()
        .build()?;
    runtime.block_on(async_main())
}

fn raise_nofile_limit() {
    let target = std::env::var("NOFILE_LIMIT")
        .ok()
        .and_then(|s| s.parse::<libc::rlim_t>().ok())
        .unwrap_or(262_144);

    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            tracing::warn!(
                "getrlimit(RLIMIT_NOFILE) failed: {}",
                std::io::Error::last_os_error()
            );
            return;
        }

        if limit.rlim_cur >= target {
            tracing::info!(
                "RLIMIT_NOFILE already sufficient: soft={} hard={}",
                limit.rlim_cur,
                limit.rlim_max
            );
            return;
        }

        let new_soft = target.min(limit.rlim_max);
        let new_limit = libc::rlimit {
            rlim_cur: new_soft,
            rlim_max: limit.rlim_max,
        };
        if libc::setrlimit(libc::RLIMIT_NOFILE, &new_limit) != 0 {
            tracing::warn!(
                "setrlimit(RLIMIT_NOFILE) failed: target={} soft={} hard={} error={}",
                target,
                limit.rlim_cur,
                limit.rlim_max,
                std::io::Error::last_os_error()
            );
            return;
        }

        tracing::info!(
            "raised RLIMIT_NOFILE: soft {} -> {} hard={}",
            limit.rlim_cur,
            new_soft,
            limit.rlim_max
        );
    }
}

fn env_core(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
}

#[cfg(target_os = "linux")]
fn pin_current_thread_to_core(name: &str, label: &str) {
    let Some(core) = env_core(name) else {
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
            tracing::info!("{label} pinned to cpu {core} via {name}");
        } else {
            tracing::warn!(
                "{label} failed to pin to cpu {core} via {name}: {}",
                std::io::Error::from_raw_os_error(rc)
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread_to_core(name: &str, label: &str) {
    if let Some(core) = env_core(name) {
        tracing::warn!(
            "{label} requested cpu pin {core} via {name}, but this platform does not support pthread affinity"
        );
    }
}

async fn async_main() -> anyhow::Result<()> {
    let database_url = db::database_url_from_env()
        .unwrap_or_else(|| "postgres://user:password@localhost:5432/mydb".to_string());
    let port = std::env::var("DESK_PORT").unwrap_or_else(|_| "4003".to_string());
    let public_market_data_enabled = std::env::var("DESK_PUBLIC_MARKET_DATA")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    tracing::info!("Connecting to database…");
    let pool = db::create_pool(&database_url).await?;
    db::run_migrations(&pool).await?;
    tracing::info!("Migrations applied");

    // Ensure the market-maker robot account and its API key exist.
    let robot_api_key =
        std::env::var("ROBOT_API_KEY").unwrap_or_else(|_| "robot_ak_2026_lightningx".to_string());
    match lightning_exchange::desk::user_service::ensure_robot_api_key(
        &pool,
        "robot@lightningx.exchange",
        "robot_secret_2026",
        &robot_api_key,
        "Market Maker Robot",
    )
    .await
    {
        Ok(uid) => tracing::info!("Robot account ready (user_id={uid}, api_key={robot_api_key})"),
        Err(e) => tracing::warn!("Robot account setup failed: {e}"),
    }

    // Pre-load all account balances into memory so GET /api/balances never touches DB.
    let account_cache: AccountCache = AccountCache::default();
    {
        let rows: Vec<(i64, String, i64, i64)> =
            sqlx::query_as("SELECT user_id, asset, balance_atoms, frozen_atoms FROM accounts")
                .fetch_all(&pool)
                .await
                .unwrap_or_default();
        for (uid, asset, bal, frz) in rows {
            account_cache
                .entry(uid)
                .or_insert_with(HashMap::new)
                .insert(asset, AccountBalance::from_atoms(bal, frz));
        }
        tracing::info!("Account cache loaded ({} rows)", account_cache.len());
    }

    // Preload VWAP from the trades table so /api/positions returns the
    // correct entry_price even immediately after restart (before any new
    // fills land). Single PG aggregate at boot — never touches PG again.
    let vwap_cache: lightning_exchange::api::VwapCache =
        std::sync::Arc::new(dashmap::DashMap::new());
    {
        let rows: Vec<(i64, String, f64, f64)> = sqlx::query_as(
            "SELECT o.user_id, SPLIT_PART(t.symbol, '_', 1) AS base_asset,
                    SUM(t.price * t.quantity) AS weighted_sum,
                    SUM(t.quantity)            AS qty_sum
             FROM trades t JOIN orders o ON o.id = t.buy_order_id
             GROUP BY o.user_id, base_asset",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
        for (uid, base, w, q) in rows {
            vwap_cache.insert(
                (uid, base),
                lightning_exchange::api::VwapStats {
                    weighted_sum: w,
                    qty_sum: q,
                },
            );
        }
        tracing::info!("VWAP cache loaded ({} (user, base) keys)", vwap_cache.len());
    }

    let open_order_meta: std::sync::Arc<DashMap<u64, OrderRuntimeMeta>> =
        std::sync::Arc::new(DashMap::new());
    let forwarded_order_origin: std::sync::Arc<DashMap<u64, u16>> =
        std::sync::Arc::new(DashMap::new());
    let forwarded_public_to_engine: std::sync::Arc<DashMap<u64, u64>> =
        std::sync::Arc::new(DashMap::new());
    let forwarded_engine_to_public: std::sync::Arc<DashMap<u64, u64>> =
        std::sync::Arc::new(DashMap::new());
    {
        // Preload all open orders into the runtime meta cache so cancel/fill
        // paths can resolve user_id + freeze_price + qty + side without a PG
        // hit. Surviving restart-time orders may briefly have stale `filled`
        // until the next event for them lands.
        let rows: Vec<(i64, i64, String, String, Option<f64>, f64, f64)> = sqlx::query_as(
            "SELECT id, user_id, symbol, side,
                    COALESCE(freeze_price, COALESCE(price, 0.0)) AS freeze_price,
                    quantity, filled
             FROM orders WHERE status IN ('PENDING','TRADING')",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
        for (id, user_id, symbol, side, freeze_price, quantity, filled) in rows {
            let mut sym_bytes = [0u8; 16];
            let sb = symbol.as_bytes();
            let n = sb.len().min(16);
            sym_bytes[..n].copy_from_slice(&sb[..n]);
            let side_byte: u8 = if side == "buy" { 0 } else { 1 };
            open_order_meta.insert(
                id as u64,
                OrderRuntimeMeta {
                    user_id,
                    freeze_price: freeze_price.unwrap_or(0.0),
                    quantity,
                    filled,
                    side: side_byte,
                    symbol: sym_bytes,
                    liq_price_ticks: 0,
                    initial_margin_atoms: 0, // pre-existing open orders: margin state unknown after restart
                },
            );
        }
        tracing::info!(
            "Order runtime cache preloaded ({} open orders)",
            open_order_meta.len()
        );
    }

    // ── Aeron setup ───────────────────────────────────────────────────────────
    let client = Arc::new(
        AeronClient::new(&aeron_dir())
            .map_err(|e| anyhow::anyhow!("Aeron init failed: {:?}", e))?,
    );
    let counter_forward_cmd_client = Arc::new(
        AeronClient::new(&aeron_dir())
            .map_err(|e| anyhow::anyhow!("CounterForward cmd Aeron init failed: {:?}", e))?,
    );
    let counter_forward_resp_client = Arc::new(
        AeronClient::new(&aeron_dir())
            .map_err(|e| anyhow::anyhow!("CounterForward resp Aeron init failed: {:?}", e))?,
    );
    let desk_id: u64 = std::env::var("DESK_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let response_stream_id = order_update_stream_for_desk(desk_id as u16);

    // PR2 dual-write: every DB-worker mutation also publishes the
    // corresponding PersistEvent to the persist Aeron stream, so the
    // (independent) redis-writer consumer keeps Redis L1 in sync.
    let persist_queue_cap: usize = std::env::var("PERSIST_QUEUE_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let persist_pub: Arc<crossbeam_queue::ArrayQueue<PersistFrame>> =
        Arc::new(crossbeam_queue::ArrayQueue::new(persist_queue_cap));
    {
        let persist_rx = persist_pub.clone();
        let mut persist_publisher =
            PersistPublisher::new(client.clone(), PERSIST_CHANNEL, PERSIST_STREAM)
                .map_err(|e| anyhow::anyhow!("PersistPublisher: {}", e))?;
        let persist_spin = std::env::var("PERSIST_SPIN")
            .map(|v| v == "1")
            .unwrap_or(false);
        let thread_name_persist = format!("d{desk_id}-persist");
        std::thread::Builder::new()
            .name(thread_name_persist.clone())
            .spawn(move || {
                pin_current_thread_to_core("DESK_PERSIST_CORE", &thread_name_persist);
                // Journal: record the persist stream on the Aeron Archive
                // when EXCHANGE_ARCHIVE_CONTROL is set. Uses a DEDICATED
                // AeronClient owned by this thread (repo convention: one
                // client per thread) so the archive control session never
                // races another thread's conductor (see aeron-wrapper
                // archive threading contract). Control traffic happens only
                // here at startup; the recording itself is maintained by the
                // archive driver. The handle must stay alive.
                let mut _journal_recorder =
                    match lightning_exchange::transport::journal::archive_config_from_env() {
                        Some(cfg) => {
                            let journal_client = std::sync::Arc::new(
                                aeron_wrapper::AeronClient::new(&aeron_dir())
                                    .expect("journal AeronClient"),
                            );
                            match lightning_exchange::transport::journal::JournalRecorder::start(
                                &journal_client,
                                &cfg,
                                PERSIST_CHANNEL,
                                PERSIST_STREAM,
                            ) {
                                // Keep the dedicated client alive alongside the recorder.
                                Ok(r) => Some((r, journal_client)),
                                Err(e) => {
                                    // Journaling was explicitly requested:
                                    // silently degrading durability is worse
                                    // than failing loudly.
                                    panic!("journal recording requested but failed: {e}");
                                }
                            }
                        }
                        None => None,
                    };
                let mut idle_us: u64 = 10;
                let journal_retention =
                    lightning_exchange::transport::journal::retention_hours_from_env();
                let mut last_journal_purge = std::time::Instant::now();
                // Per-publisher monotonic sequence, assigned here — the single
                // drain point — so consumers can checkpoint (publisher_id, seq)
                // and dedup on replay/restart.
                //
                // Seeded from wall-clock nanos so a restarted desk always
                // resumes ABOVE its previous range (frames/sec << ns/sec by
                // many orders of magnitude): a fresh process can never emit
                // sequences the consumers' checkpoints would discard as dups.
                // Consumers treat a huge forward jump as a publisher restart,
                // not a loss gap.
                let mut persist_seq: u64 = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(1);
                loop {
                    let mut did_work = false;
                    while let Some(frame) = persist_rx.pop() {
                        did_work = true;
                        persist_seq += 1;
                        let mut sequenced = frame;
                        sequenced.publisher_id = desk_id as u16;
                        sequenced.seq = persist_seq;
                        let _ = persist_publisher.publish(&sequenced);
                    }
                    // Journal retention: purge stopped recordings beyond the
                    // horizon, hourly, on this (archive-owning) thread.
                    if let (Some(hours), Some((recorder, _))) =
                        (journal_retention, _journal_recorder.as_mut())
                    {
                        if last_journal_purge.elapsed().as_secs() >= 3600 {
                            last_journal_purge = std::time::Instant::now();
                            let cutoff = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as i64)
                                .unwrap_or(0)
                                - (hours as i64) * 3_600_000;
                            match recorder.purge_stopped_before("ipc", PERSIST_STREAM, cutoff) {
                                Ok((0, _)) => {}
                                Ok((n, seg)) => tracing::info!(
                                    "journal retention: purged {n} recording(s), {seg} segment(s)"
                                ),
                                Err(e) => tracing::warn!("journal retention failed: {e}"),
                            }
                        }
                    }
                    if !did_work {
                        if persist_spin {
                            std::hint::spin_loop();
                        } else {
                            std::thread::sleep(std::time::Duration::from_micros(idle_us));
                            idle_us = (idle_us * 2).min(500);
                        }
                    } else {
                        idle_us = 10;
                    }
                }
            })?;
    }

    // Per-symbol order publishers: each symbol routes to its own Aeron stream so the
    // matching threads never share a stream and there is zero HOL blocking between symbols.
    let symbols_env =
        std::env::var("SYMBOLS").unwrap_or_else(|_| "ETH_USDT,BTC_USDT,SOL_USDT".to_string());
    let order_pubs: std::collections::HashMap<String, DeskOrderPublisher> = symbols_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|sym| {
            let stream = orders_stream_for_symbol(&sym);
            let pub_ = DeskOrderPublisher::new(client.clone(), &orders_channel(), stream)
                .unwrap_or_else(|e| panic!("DeskOrderPublisher({sym}): {e}"));
            (sym, pub_)
        })
        .collect();

    let mut order_update_sub =
        DeskOrderUpdateSubscriber::new(client.clone(), &order_update_channel(), response_stream_id)
            .map_err(|e| anyhow::anyhow!("DeskOrderUpdateSubscriber: {}", e))?;

    let mut counter_forward_cmd_sub = CounterForwardSubscriber::new(
        counter_forward_cmd_client.clone(),
        &counter_forward_channel(),
        counter_forward_cmd_stream_for_desk(desk_id as u16),
    )
    .map_err(|e| anyhow::anyhow!("CounterForward cmd subscriber: {}", e))?;
    let mut counter_forward_resp_sub = CounterForwardSubscriber::new(
        counter_forward_resp_client.clone(),
        &counter_forward_channel(),
        counter_forward_resp_stream_for_desk(desk_id as u16),
    )
    .map_err(|e| anyhow::anyhow!("CounterForward resp subscriber: {}", e))?;

    let public_market_data_subs = if public_market_data_enabled {
        Some((
            DeskTradeSubscriber::new(client.clone(), &trade_channel(), TRADE_STREAM)
                .map_err(|e| anyhow::anyhow!("DeskTradeSubscriber: {}", e))?,
            DeskDepthSubscriber::new(
                client.clone(),
                &depth_channel(),
                DEPTH_STREAM,
                DEPTH50_STREAM,
                LEVEL2_STREAM,
            )
            .map_err(|e| anyhow::anyhow!("DeskDepthSubscriber: {}", e))?,
        ))
    } else {
        tracing::info!("desk-server public market-data path disabled; use market-data-gateway");
        None
    };

    tracing::info!("Aeron subscribers and publisher created");

    // ── Latency tracer (optional — disabled if is not running) ────────
    // Gated by TRACER_ENABLED env (default = off). Each hot-path call site
    // pays one `if let Some` check either way, but when the env is "0" we
    // never spawn the Aeron publisher + drain thread + per-msg mpsc send.
    // At 400K conns the live tracer drove desk-server recv-spin into a
    // 300×-slower regime (see exchange_engine.rs comment); leaving it off
    // by default and flipping on for diagnosis.
    let tracer = if std::env::var("TRACER_ENABLED")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        spawn_tracer(
            &aeron_dir(),
            METRICS_CHANNEL,
            METRICS_STREAM,
            DESK_INSTANCE_ID,
        )
        .map(Arc::new)
    } else {
        None
    };
    if tracer.is_some() {
        tracing::info!(
            "Exchange tracer connected (instance_id={})",
            DESK_INSTANCE_ID
        );
    }

    // ── Command channel: async WS handlers → Aeron spin thread ───────────────
    // Lock-free MPMC bounded ring. AeronCmd is ~2 KB (SmallVec inline storage),
    // so 65 536 slots = ~128 MB — enough to absorb a 10 K-connection burst
    // while still bounded. Old default was 5 M × 2 KB = 9.8 GB (!).
    // Override via AERON_CMD_CAP env.
    let aeron_cmd_cap: usize = std::env::var("AERON_CMD_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(65_536);
    let aeron_cmd_ring: std::sync::Arc<crossbeam_queue::ArrayQueue<AeronCmd>> =
        std::sync::Arc::new(crossbeam_queue::ArrayQueue::new(aeron_cmd_cap));
    let aeron_cmd_tx = aeron_cmd_ring.clone();
    let aeron_cmd_rx = aeron_cmd_ring;
    // Clone for DB worker so it can cancel orders when fund freeze fails post-ACCEPTED.
    let db_worker_aeron_tx = aeron_cmd_tx.clone();

    // Priority ring for system-generated liquidation orders.
    // Small (1024) — the risk tick fires at most a handful of liquidation orders per 10 ms.
    // Drained by the spin thread BEFORE aeron_cmd_rx so liq orders always go first.
    let liq_cmd_ring: std::sync::Arc<crossbeam_queue::ArrayQueue<AeronCmd>> =
        std::sync::Arc::new(crossbeam_queue::ArrayQueue::new(1024));
    let liq_cmd_tx = liq_cmd_ring.clone();
    let liq_cmd_rx = liq_cmd_ring;

    let counter_forward_cap: usize = std::env::var("COUNTER_FORWARD_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(65_536);
    let counter_forward_ring: std::sync::Arc<
        crossbeam_queue::ArrayQueue<
            lightning_exchange::transport::counter_forward::CounterForwardMsg,
        >,
    > = std::sync::Arc::new(crossbeam_queue::ArrayQueue::new(counter_forward_cap));
    let counter_forward_tx = counter_forward_ring.clone();
    let counter_forward_rx = counter_forward_ring;

    // ── Sync next_order_id from DB so WS atomic IDs don't collide ─────────────
    // Each desk-server has its own AtomicU64 counter — without DESK_ID
    // offsetting, two desks both starting from MAX(id)+1 hand out the
    // same order_ids and engine rejects half with DuplicateOrderId.
    // DESK_ID is a small integer (0,1,2,…); each desk reserves a 1B-id
    // slab starting at base + DESK_ID*1e9. 1B IDs per desk is enough
    // for ~32 years at 1k orders/s sustained, and 32 desks × 1B is
    // well within u64.
    let max_db_id: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM orders")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    let initial_id = (max_db_id as u64) + 1 + desk_id * 1_000_000_000;
    tracing::info!(
        "next_order_id initial: max_db_id={} desk_id={} initial_id={}",
        max_db_id,
        desk_id,
        initial_id,
    );

    // ── Shared state ──────────────────────────────────────────────────────────
    // read_pool must be created first: its market_senders() initialise MarketFanout.
    let read_pool = std::sync::Arc::new(lightning_exchange::read_actor::ReadActorPool::new());
    let market_fanout = std::sync::Arc::new(
        lightning_exchange::api::MarketFanout::new_with_actors(read_pool.market_senders()),
    );

    let valid_symbols: std::collections::HashSet<String> = order_pubs.keys().cloned().collect();

    // Redis L1: cheap multiplexed conn for REST read fallback. If REDIS_URL is
    // unreachable at boot, leave it as None — handlers transparently fall
    // back to the existing PG path. We do NOT block startup on Redis.
    let redis_conn: Option<redis::aio::MultiplexedConnection> = {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
        match redis::Client::open(url.clone()) {
            Ok(client) => match client.get_multiplexed_async_connection().await {
                Ok(mut c) => match redis::cmd("PING").query_async::<String>(&mut c).await {
                    Ok(_) => {
                        tracing::info!("Redis L1 reader attached: {url}");
                        Some(c)
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Redis PING failed at {url}: {e} — REST will fall back to PG"
                        );
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        "Redis connect failed at {url}: {e} — REST will fall back to PG"
                    );
                    None
                }
            },
            Err(e) => {
                tracing::warn!("Invalid REDIS_URL '{url}': {e} — REST will fall back to PG");
                None
            }
        }
    };

    let risk_engine = lightning_exchange::desk::risk::RiskEngine::new();
    for entry in account_cache.iter() {
        let user_id = *entry.key();
        // S2: the risk engine speaks atoms — seed the ledger value verbatim.
        let usdt_atoms = entry
            .value()
            .get("USDT")
            .map(|amount| amount.balance_atoms)
            .unwrap_or(0);
        risk_engine.initialize_account(user_id, usdt_atoms);
    }
    tracing::info!(
        "Risk engine initialized ({} accounts)",
        risk_engine.account_count()
    );
    // S1.4: durable margin state overrides the naive balance seed. Rows in
    // risk_accounts/positions are authoritative — a desk that died with
    // open positions comes back field-equivalent (mark price re-converges
    // within one tick). Refusing to start on hydrate failure is deliberate:
    // trading against silently-forgotten positions is the one unrecoverable
    // mistake here.
    // Startup-race guard: let pg-writer finish catching up the journal
    // tail before reading the margin tables (stale hydrate = trading
    // against forgotten fills). Quiet floors twice in a row ≈ caught up.
    if !lightning_exchange::desk::risk_persist::wait_for_writer_quiesce(
        &pool,
        std::time::Duration::from_millis(500),
        std::time::Duration::from_secs(10),
    )
    .await
    {
        tracing::warn!(
            "pg-writer checkpoints still advancing after 10s — hydrating anyway              (reconcile will flag any staleness)"
        );
    }
    match lightning_exchange::desk::risk_persist::hydrate_from_pg(&risk_engine, &pool).await {
        Ok(stats) => tracing::info!(
            "Risk engine hydrated: {} accounts, {} positions, insurance fund {} cents",
            stats.accounts,
            stats.positions,
            stats.insurance_fund_atoms
        ),
        Err(e) => panic!("risk hydrate failed — refusing to trade blind: {e}"),
    }

    // Per-user order-entry rate limiter (WS_RL_* env; off by default).
    // Restore persisted buckets so a restart doesn't mint fresh budgets,
    // and snapshot them back to Redis every 2s.
    let rate_limiter = std::sync::Arc::new(
        lightning_exchange::rate_limit::SharedRateLimiter::from_env(),
    );
    if rate_limiter.is_enabled() {
        if let Some(conn) = redis_conn.clone() {
            let mut conn_restore = conn.clone();
            match lightning_exchange::desk::redis_store::load_rate_buckets(&mut conn_restore).await
            {
                Ok(entries) if !entries.is_empty() => {
                    rate_limiter.restore(&entries);
                    tracing::info!("rate limiter: restored {} buckets", entries.len());
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("rate limiter: restore failed: {e}"),
            }
            let limiter_snap = rate_limiter.clone();
            let mut conn_snap = conn;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
                loop {
                    tick.tick().await;
                    let snapshot = limiter_snap.snapshot();
                    if let Err(e) = lightning_exchange::desk::redis_store::store_rate_buckets(
                        &mut conn_snap,
                        &snapshot,
                    )
                    .await
                    {
                        tracing::warn!("rate limiter: snapshot failed: {e}");
                    }
                }
            });
        } else {
            tracing::warn!("rate limiter enabled but Redis unavailable — buckets are memory-only");
        }
    }

    // Revocation propagation: refresh the in-memory revoked set from Redis
    // every 30s (auth checks stay synchronous and IO-free).
    if let Some(conn) = redis_conn.clone() {
        let mut conn = conn;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tick.tick().await;
                if let Err(e) =
                    lightning_exchange::user_service::refresh_revocations(&mut conn).await
                {
                    tracing::warn!("revocation refresh failed: {e}");
                }
            }
        });
    }

    // A5: periodic conservation check — logs SUM(balance_atoms) every 5 min.
    // Operators compare this to total_deposits - withdrawals - fees to verify
    // zero-sum invariant. A divergence indicates a double-credit/double-debit bug.
    {
        let pool_c = pool.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(300));
            loop {
                tick.tick().await;
                match lightning_exchange::desk::account_repository::AccountRepository::new(&pool_c)
                    .sum_all_balance_atoms()
                    .await
                {
                    Ok((sum_bal, sum_frz, rows)) => tracing::info!(
                        "conservation check: sum_balance_atoms={} sum_frozen_atoms={} account_rows={}",
                        sum_bal, sum_frz, rows
                    ),
                    Err(e) => tracing::warn!("conservation check failed: {e}"),
                }
            }
        });
    }

    // ── S4: index aggregation (external sources → median, outlier-
    //    rejected; below quorum → mark freezes). No INDEX_SOURCES env →
    //    None → raw-mid marks (dev mode). ──────────────────────────────
    let (index_agg, mark_frozen_count): (
        Option<Arc<lightning_exchange::desk::index_price::IndexAggregator>>,
        Arc<std::sync::atomic::AtomicU64>,
    ) = {
        let symbols: Vec<String> = std::env::var("SYMBOLS")
            .unwrap_or_else(|_| "ETH_USDT,BTC_USDT,SOL_USDT".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let sources =
            lightning_exchange::desk::index_price::spawn_http_pollers(symbols);
        let agg = if sources.is_empty() {
            tracing::warn!(
                "INDEX_SOURCES is empty -- using raw book mid; liquidations disabled until real mark arrives"
            );
            None
        } else {
            tracing::info!("S4: index aggregation over {} source(s)", sources.len());
            Some(Arc::new(
                lightning_exchange::desk::index_price::IndexAggregator::new(sources),
            ))
        };
        (agg, Arc::new(std::sync::atomic::AtomicU64::new(0)))
    };
    // B4: flag shared with the risk-tick task; true when INDEX_SOURCES is empty.
    let index_sources_empty = index_agg.is_none();
    // B5: track last mark update time per symbol (spin thread writes, risk tick reads).
    let last_mark_update: Arc<DashMap<[u8; 16], std::time::Instant>> =
        Arc::new(DashMap::new());
    let mark_freeze_threshold_secs: u64 = std::env::var("MARK_FREEZE_THRESHOLD_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    // S4.5: index/mark health on /metrics — the runbook alarms on
    // index_frozen_total and mark_update_frozen_total growth.
    if let Some(agg) = &index_agg {
        let a = agg.clone();
        lightning_exchange::metrics::register_gauge("index_aggregations_ok_total", move || {
            a.health.ok.load(std::sync::atomic::Ordering::Relaxed) as f64
        });
        let a = agg.clone();
        lightning_exchange::metrics::register_gauge("index_frozen_total", move || {
            a.health.frozen.load(std::sync::atomic::Ordering::Relaxed) as f64
        });
        let a = agg.clone();
        lightning_exchange::metrics::register_gauge("index_outliers_dropped_total", move || {
            a.health.outliers_dropped.load(std::sync::atomic::Ordering::Relaxed) as f64
        });
    }
    {
        let c = mark_frozen_count.clone();
        lightning_exchange::metrics::register_gauge("mark_update_frozen_total", move || {
            c.load(std::sync::atomic::Ordering::Relaxed) as f64
        });
    }

    // ── T2: runtime exchange controls (halt/fees), hydrated from PG ───
    let exchange_config = lightning_exchange::desk::exchange_config::ExchangeConfig::new();
    match exchange_config.hydrate(&pool).await {
        Ok(n) => tracing::info!("exchange_config hydrated: {n} symbol control row(s)"),
        Err(e) => tracing::warn!("exchange_config hydrate failed: {e}"),
    }

    // ── S5: trigger-order books (hydrated + fired by the task below,
    //    after AppState exists so it can reach the cmd queue) ───────────
    let trigger_books: Arc<
        DashMap<String, parking_lot::Mutex<lightning_exchange::desk::trigger::TriggerBook>>,
    > = Arc::new(DashMap::new());

    // ── S3: funding scheduler/sampler task ────────────────────────────
    // Durable schedule anchor = funding_state (advanced by pg-writer in
    // the settlement transaction); the desk resumes from it on startup.
    let funding_view: Arc<DashMap<String, (i64, i64, i64)>> = Arc::new(DashMap::new());
    {
        use lightning_exchange::desk::funding::{
            FundingConfig, FundingScheduler, PremiumTracker, apply_in_memory,
            compute_settlement, load_funding_schedule,
        };
        let cfg = FundingConfig::from_env().expect("invalid FUNDING_CLAMP_E9 or FUNDING_INTEREST_E9 env var");
        let persisted = load_funding_schedule(&pool).await.unwrap_or_default();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let sample_secs: u64 = std::env::var("FUNDING_SAMPLE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);
        let symbols: Vec<String> = std::env::var("SYMBOLS")
            .unwrap_or_else(|_| "ETH_USDT,BTC_USDT,SOL_USDT".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let mut books: Vec<(String, [u8; 16], FundingScheduler, PremiumTracker)> = symbols
            .iter()
            .map(|sym| {
                let sched = FundingScheduler::new(persisted.get(sym).copied(), now_ms, &cfg);
                (
                    sym.clone(),
                    lightning_exchange::transport::persist_event::pack_str(sym),
                    sched,
                    PremiumTracker::default(),
                )
            })
            .collect();
        for (sym, _, sched, _) in &books {
            funding_view.insert(sym.clone(), (sched.next_settlement_at_ms, 0, 0));
        }
        let view = funding_view.clone();
        let index_agg_f = index_agg.clone();
        let engine_f = risk_engine.clone();
        let persist_f = persist_pub.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_sample = std::time::Instant::now();
            loop {
                tick.tick().await;
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                let do_sample = last_sample.elapsed().as_secs() >= sample_secs;
                if do_sample {
                    last_sample = std::time::Instant::now();
                }
                for (sym, sym16, sched, tracker) in books.iter_mut() {
                    let Some(mark) = engine_f.mark_price_ticks(sym16) else {
                        continue; // no market yet — nothing to sample/settle
                    };
                    if do_sample {
                        // S4: real index when configured; without it the
                        // mark doubles as index → premium 0, interest-only
                        // funding (dev mode). Frozen index → skip sample.
                        let index = match index_agg_f.as_ref() {
                            Some(agg) => {
                                use lightning_exchange::desk::funding::IndexPriceSource;
                                match agg.index_price_ticks(sym16) {
                                    Some(i) => i,
                                    None => continue, // frozen — never guess
                                }
                            }
                            None => mark,
                        };
                        tracker.sample(mark, index);
                        if let Some(mut v) = view.get_mut(sym) {
                            v.2 = tracker.twap_e9();
                        }
                    }
                    while let Some(boundary_ms) = sched.due(now_ms) {
                        let cfg = FundingConfig::from_env().unwrap_or_else(|e| {
                            tracing::error!("FundingConfig::from_env failed: {e}; using defaults");
                            FundingConfig { period_secs: 28_800, interest_e9: 100_000, clamp_e9: 7_500_000 }
                        });
                        let rate_e9 = tracker.close_period(&cfg);
                        let rules =
                            lightning_exchange::desk::symbol_rules::SymbolRules::for_symbol(sym);
                        // Settlement set = this engine's positions NOW;
                        // pg-writer recomputes over the table at the same
                        // sequence point — identical by construction.
                        let positions: Vec<_> = engine_f
                            .positions
                            .iter()
                            .filter(|e| &e.key().1 == sym16)
                            .map(|e| (e.key().0, e.value().side, e.value().qty_lots))
                            .collect();
                        let settlement = compute_settlement(
                            positions.into_iter(),
                            mark,
                            rules.notional_scale,
                            rate_e9,
                        );
                        apply_in_memory(&engine_f, &settlement);
                        let frame = lightning_exchange::transport::persist_event::PersistFrame::funding_settled(
                            lightning_exchange::transport::persist_event::FundingSettledPayload {
                                symbol: *sym16,
                                rate_e9,
                                mark_price_ticks: mark,
                                notional_scale: rules.notional_scale,
                                settled_at_ms: boundary_ms,
                                next_settlement_at_ms: sched.next_settlement_at_ms,
                            },
                        );
                        publish_frame(&persist_f, &frame);
                        view.insert(
                            sym.clone(),
                            (sched.next_settlement_at_ms, rate_e9, 0),
                        );
                        tracing::info!(
                            "funding settled: {} rate_e9={} users={} residue={} next={}",
                            sym,
                            rate_e9,
                            settlement.deltas.len(),
                            settlement.residue_atoms,
                            sched.next_settlement_at_ms
                        );
                    }
                }
            }
        });
    }

    let state = AppState {
        db: Arc::new(pool),
        rate_limiter: rate_limiter.clone(),
        engines: None,
        market_fanout: market_fanout.clone(),
        public_market_data_enabled,
        response_stream_id,
        desk_id: desk_id as u16,
        user_tx: Arc::new(lightning_exchange::api::UserTxRegistry::new()),
        next_order_id: Arc::new(AtomicU64::new(initial_id)),
        aeron_cmd_tx: Some(aeron_cmd_tx),
        counter_forward_tx: Some(counter_forward_tx),
        liq_cmd_tx: Some(liq_cmd_tx),
        pending_meta: Arc::new(DashMap::new()),
        pending_orders: Arc::new(DashMap::new()),
        last_depth: Arc::new(DashMap::new()),
        last_ticker: Arc::new(DashMap::new()),
        last_trade_price: Arc::new(DashMap::new()),
        tracer: tracer.clone(),
        account_cache: account_cache.clone(),
        valid_symbols: Arc::new(valid_symbols),
        redis: redis_conn,
        persist_pub: Some(persist_pub.clone()),
        funding_view: funding_view.clone(),
        trigger_books: trigger_books.clone(),
        exchange_config: exchange_config.clone(),
        vwap_cache: vwap_cache.clone(),
        write_pool: Arc::new(lightning_exchange::write_actor::WriteActorPool::new()),
        read_pool,
        risk_engine: risk_engine.clone(),
    };

    // DB worker thread removed. PR5 took every PG SQL call out of
    // process_db_cmd; what's left (publish_frame + DashMap updates +
    // user_tx.try_send) is all sync µs-scale work that's now called
    // directly from the recv-spin thread. Saves one thread + one rtrb hop.
    let _ = db_worker_aeron_tx; // legacy cancel route; kept for ABI parity.

    // DESK_SPIN=false → exponential backoff (EC2/CPU-constrained hosts).
    // Default: spin_loop() for lowest latency on dedicated cores.
    let use_spin = std::env::var("DESK_SPIN")
        .map(|v| v != "false")
        .unwrap_or(true);

    // ── Aeron SEND spin: single thread, owns the Publishers directly ─────────
    // Phase 2 experiment (N parallel send-spins behind Mutex<Publisher>)
    // reverted: shards=4 made things WORSE at 400K conn × 1 op/s on macOS
    // (place avg 345 ms → 499 ms). Mutex contention + CPU starvation of
    // MM/pressure tokio workers outweighed the parallelism. Bottleneck
    // is somewhere else (likely OS scheduler tail + WS/TCP overhead at
    // 350 K parked conns). Future shard experiment lives in git history.

    // Pre-create all counter-forward publications here, before any spin thread
    // starts. This serializes aeronmd registration requests (no concurrent
    // add_publication from multiple threads), and prevents the SIGSEGV that
    // occurred when the spin thread called add_publication while other Aeron
    // I/O was in flight on the same client.
    let cf_channel = counter_forward_channel();
    use lightning_exchange::desk::counter_shard::COUNTER_SHARD_COUNT;
    let send_cf_cmd_pubs: HashMap<u16, CounterForwardPublisher> = (0..COUNTER_SHARD_COUNT)
        .map(|desk| {
            let stream = counter_forward_cmd_stream_for_desk(desk);
            let pub_ = new_counter_forward_publisher_with_retry(
                counter_forward_cmd_client.clone(),
                &cf_channel,
                stream,
                "cmd",
            );
            (desk, pub_)
        })
        .collect();
    let send_cf_resp_pubs: HashMap<u16, CounterForwardPublisher> = (0..COUNTER_SHARD_COUNT)
        .map(|desk| {
            let stream = counter_forward_resp_stream_for_desk(desk);
            let pub_ = new_counter_forward_publisher_with_retry(
                counter_forward_cmd_client.clone(),
                &cf_channel,
                stream,
                "send-resp",
            );
            (desk, pub_)
        })
        .collect();

    {
        let aeron_cmd_rx = aeron_cmd_rx.clone();
        let liq_cmd_rx = liq_cmd_rx.clone();
        let mut order_pubs = order_pubs;
        let order_meta_cache = open_order_meta.clone();
        let forward_origin_send = forwarded_order_origin.clone();
        let public_to_engine_send = forwarded_public_to_engine.clone();
        let pending_meta = state.pending_meta.clone();
        let user_tx = state.user_tx.clone();
        let account_cache_send = account_cache.clone();
        let risk_engine_send = state.risk_engine.clone();
        let counter_forward_channel_send = cf_channel.clone();
        let spin_tracer = tracer.clone();
        let queue_metrics = std::env::var("DESK_QUEUE_METRICS")
            .map(|v| v == "1")
            .unwrap_or(false);
        let counter_forward_drain_budget: usize = std::env::var("COUNTER_FORWARD_DRAIN_BUDGET")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256);
        let exchange_config_send = exchange_config.clone();
        // S6.3: leverage tier table (RISK_TIERS env, e.g. "1000000:20,5000000:10,:5").
        let risk_tiers_send =
            lightning_exchange::desk::risk::calc::parse_risk_tiers(
                &std::env::var("RISK_TIERS").unwrap_or_default(),
            );
        let mut counter_forward_cmd_pubs = send_cf_cmd_pubs;
        let mut counter_forward_resp_pubs = send_cf_resp_pubs;
        let thread_name_send = format!("d{desk_id}-send");
        std::thread::Builder::new()
            .name(thread_name_send.clone())
            .spawn(move || {
                pin_current_thread_to_core("DESK_SEND_CORE", &thread_name_send);
                let mut idle_us: u64 = 0;
                let mut metric_last = std::time::Instant::now();
                let mut metric_drained: u64 = 0;
                let mut metric_max_len: usize = 0;
                let counter_forward_publish_failures = AtomicU64::new(0);
                loop {
                    let mut did_work = false;
                    counter_forward_cmd_sub.do_work();
                    if queue_metrics {
                        metric_max_len = metric_max_len.max(aeron_cmd_rx.len());
                    }
                    // Priority: drain liquidation orders before normal orders.
                    while let Some(cmd) = liq_cmd_rx.pop() {
                        did_work = true;
                        match cmd {
                            AeronCmd::NewOrder(req) => {
                                // Copy packed fields to locals before any reference (packed struct).
                                let order_id: u64 = req.client_order_id;
                                let participant_id: u64 = req.participant_id;
                                let quantity_lots: i64 = req.quantity_lots;
                                let side: u8 = req.side;
                                let symbol: [u8; 16] = req.symbol;
                                let sym = std::str::from_utf8(&symbol)
                                    .unwrap_or("")
                                    .trim_end_matches('\0');
                                let sym_rules = lightning_exchange::symbol_rules::SymbolRules::for_symbol(sym);
                                // Retrieve liq_price_ticks from pending_meta (set by tick task).
                                let liq_ticks = pending_meta
                                    .get(&order_id)
                                    .map(|m| m.liq_price_ticks)
                                    .unwrap_or(0);
                                remember_runtime_order(
                                    &order_meta_cache,
                                    order_id,
                                    participant_id as i64,
                                    0.0, // liq orders: no quote-asset freeze
                                    sym_rules.lots_to_quantity(quantity_lots),
                                    side,
                                    symbol,
                                    0, // liq close side has no margin reservation
                                );
                                if liq_ticks != 0 {
                                    if let Some(mut m) = order_meta_cache.get_mut(&order_id) {
                                        m.liq_price_ticks = liq_ticks;
                                    }
                                }
                                match order_pubs.get_mut(sym) {
                                    Some(pub_) => {
                                        if let Err(e) = pub_.publish_new_order(&req) {
                                            tracing::error!(
                                                "liq-ring publish failed for order {order_id} ({sym}): {e:?}"
                                            );
                                        }
                                    }
                                    None => tracing::error!(
                                        "liq-ring: no order publisher for symbol '{sym}' (order {order_id})"
                                    ),
                                }
                            }
                            _ => {} // liquidation ring carries only NewOrder
                        }
                    }
                    for _ in 0..counter_forward_drain_budget {
                        let Some(msg) = counter_forward_cmd_sub.poll() else {
                            break;
                        };
                        did_work = true;
                        match msg {
                            CounterForwardMsg::NewOrder(fwd) => {
                                let ingress_desk_id = fwd.ingress_desk_id;
                                let req = fwd.req;
                                let meta = fwd.meta;
                                let order_id = req.client_order_id;
                                let user_id = req.participant_id as i64;
                                counter_forward_debug!(
                                    "cf_owner_new desk={} ingress={} user={} public_order={}",
                                    desk_id,
                                    ingress_desk_id,
                                    user_id,
                                    order_id
                                );
                                let symbol = req.symbol;
                                let sym = std::str::from_utf8(&symbol)
                                    .unwrap_or("")
                                    .trim_end_matches('\0');
                                let (_, quote_asset) = sym.split_once('_').unwrap_or(("BTC", "USDT"));
                                let initial_margin_atoms = meta.initial_margin_atoms;
                                // atoms → USDT is 1e8 (S2); the old /100.0 was a
                                // cents-era leftover that inflated the freeze by
                                // 1e6× and silently rejected every cross-desk
                                // order (found while wiring S5).
                                let margin_usdt = initial_margin_atoms as f64 / 100_000_000.0;
                                let mut reject = |reason: &str| {
                                    if let Some(frame) = CounterForwardWsFrame::new(
                                        user_id,
                                        order_id,
                                        &lightning_exchange::ws_sbe::encode_order_rejected(0, reason),
                                    ) {
                                        if let Some(pub_) = counter_forward_resp_pubs.get_mut(&ingress_desk_id) {
                                            if pub_.publish_ws_frame(&frame).is_err() {
                                                log_counter_forward_publish_failure(
                                                    &counter_forward_publish_failures,
                                                    "owner_reject_to_ingress",
                                                );
                                            }
                                        }
                                    }
                                };

                                // Position / OI caps mirror the direct WS path (O(1)).
                                let fwd_rules =
                                    lightning_exchange::symbol_rules::SymbolRules::for_symbol(sym);
                                if let Err(reason) = risk_engine_send.check_position_limit(
                                    user_id,
                                    &symbol,
                                    req.quantity_lots,
                                    fwd_rules.max_position_lots,
                                ) {
                                    reject(reason);
                                    continue;
                                }
                                if let Err(reason) = risk_engine_send.check_symbol_oi_limit(
                                    &symbol,
                                    req.quantity_lots,
                                    fwd_rules.max_symbol_oi_lots,
                                ) {
                                    reject(reason);
                                    continue;
                                }
                                // T2 halt gate: order ENTRY is blocked for a
                                // halted symbol (cancels/liquidations still
                                // flow). reduce_only orders are de-risking,
                                // so they are allowed through a halt.
                                if req.reduce_only == 0 && exchange_config_send.is_halted(sym) {
                                    reject("trading halted for this symbol");
                                    continue;
                                }
                                // reduce-only gate (testnet): reject orders
                                // that would increase exposure; cap to the
                                // position size otherwise.
                                if req.reduce_only != 0 {
                                    match risk_engine_send.check_reduce_only(
                                        user_id, &symbol, req.side, req.quantity_lots,
                                    ) {
                                        Ok(_capped) => {}
                                        Err(reason) => {
                                            reject(reason);
                                            continue;
                                        }
                                    }
                                }
                                let order_notional =
                                    lightning_exchange::desk::risk::calc::calc_notional_atoms(
                                        { let p: i64 = req.price_ticks; p },
                                        req.quantity_lots,
                                        fwd_rules.notional_scale,
                                    );
                                if let Err(reason) = risk_engine_send.check_leverage_tier(
                                    user_id,
                                    &symbol,
                                    order_notional,
                                    fwd_rules.notional_scale,
                                    &risk_tiers_send,
                                    fwd_rules.default_leverage,
                                ) {
                                    reject(reason);
                                    continue;
                                }
                                if let Err(reason) = risk_engine_send.check_and_reserve_margin(user_id, initial_margin_atoms) {
                                    reject(reason);
                                    continue;
                                }
                                if !try_freeze_cache(&account_cache_send, user_id, quote_asset, margin_usdt) {
                                    risk_engine_send.release_order_margin(user_id, initial_margin_atoms);
                                    reject("Insufficient balance");
                                    continue;
                                }
                                remember_runtime_order(
                                    &order_meta_cache,
                                    order_id,
                                    user_id,
                                    meta.freeze_price,
                                    meta.qty,
                                    req.side,
                                    symbol,
                                    initial_margin_atoms,
                                );
                                pending_meta.insert(order_id, order_meta_from_forward(meta));
                                forward_origin_send.insert(order_id, ingress_desk_id);
                                if let Some(pub_) = order_pubs.get_mut(sym) {
                                    if pub_.publish_new_order(&req).is_err() {
                                        pending_meta.remove(&order_id);
                                        forward_origin_send.remove(&order_id);
                                        remove_runtime_order(&order_meta_cache, order_id, order_id);
                                        risk_engine_send.release_order_margin(user_id, initial_margin_atoms);
                                        release_cache_frozen(&account_cache_send, user_id, quote_asset, margin_usdt);
                                        log_counter_forward_publish_failure(
                                            &counter_forward_publish_failures,
                                            "owner_to_engine_new_order",
                                        );
                                        reject("system busy");
                                    }
                                } else {
                                    pending_meta.remove(&order_id);
                                    forward_origin_send.remove(&order_id);
                                    remove_runtime_order(&order_meta_cache, order_id, order_id);
                                    risk_engine_send.release_order_margin(user_id, initial_margin_atoms);
                                    release_cache_frozen(&account_cache_send, user_id, quote_asset, margin_usdt);
                                    reject(&format!("No engine for symbol: {}", sym));
                                }
                            }
                            CounterForwardMsg::Cancel(fwd) => {
                                let mut req = fwd.req;
                                let public_order_id = req.order_id;
                                if let Some(engine_order_id) =
                                    public_to_engine_send.get(&public_order_id).map(|e| *e.value())
                                {
                                    req.order_id = engine_order_id;
                                }
                                let symbol_bytes =
                                    runtime_meta(&order_meta_cache, req.order_id).map(|m| m.symbol);
                                let routed = if let Some(sym_bytes) = symbol_bytes {
                                    let sym_end =
                                        sym_bytes.iter().position(|&b| b == 0).unwrap_or(16);
                                    let sym =
                                        std::str::from_utf8(&sym_bytes[..sym_end]).unwrap_or("");
                                    if let Some(pub_) = order_pubs.get_mut(sym) {
                                        let _ = pub_.publish_cancel(&req);
                                        true
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                };
                                if !routed {
                                    for pub_ in order_pubs.values_mut() {
                                        let _ = pub_.publish_cancel(&req);
                                    }
                                }
                            }
                            CounterForwardMsg::WsFrame(_) => {}
                        }
                    }
                    for _ in 0..counter_forward_drain_budget {
                        let Some(msg) = counter_forward_rx.pop() else {
                            break;
                        };
                        did_work = true;
                        match msg {
                            CounterForwardMsg::NewOrder(fwd) => {
                                let req = fwd.req;
                                let user_id = req.participant_id as i64;
                                let order_id = req.client_order_id;
                                let owner = lightning_exchange::desk::counter_shard::owner_shard_for_user_id(user_id);
                                if let Some(pub_) = counter_forward_cmd_pubs.get_mut(&owner) {
                                    counter_forward_debug!(
                                        "cf_ingress_new desk={} owner={} user={} public_order={}",
                                        desk_id,
                                        owner,
                                        user_id,
                                        order_id
                                    );
                                    if pub_.publish_new_order(&fwd).is_err() {
                                        log_counter_forward_publish_failure(
                                            &counter_forward_publish_failures,
                                            "ingress_to_owner_new_order",
                                        );
                                        if let Some(tx) = user_tx.get(user_id) {
                                            let _ = tx.try_send((
                                                lightning_exchange::ws_sbe::encode_order_rejected(
                                                    order_id,
                                                    "system busy",
                                                ),
                                                order_id,
                                            ));
                                        }
                                    }
                                }
                            }
                            CounterForwardMsg::Cancel(fwd) => {
                                let req = fwd.req;
                                let user_id = req.participant_id as i64;
                                let cancel_order_id = req.order_id;
                                let owner = lightning_exchange::desk::counter_shard::owner_shard_for_user_id(user_id);
                                if let Some(pub_) = counter_forward_cmd_pubs.get_mut(&owner) {
                                    counter_forward_debug!(
                                        "cf_ingress_cancel desk={} owner={} user={} public_order={}",
                                        desk_id,
                                        owner,
                                        user_id,
                                        cancel_order_id
                                    );
                                    if pub_.publish_cancel(&fwd).is_err() {
                                        log_counter_forward_publish_failure(
                                            &counter_forward_publish_failures,
                                            "ingress_to_owner_cancel",
                                        );
                                        if let Some(tx) = user_tx.get(user_id) {
                                            let _ = tx.try_send((
                                                lightning_exchange::ws_sbe::encode_error("system busy"),
                                                req.order_id,
                                            ));
                                        }
                                    }
                                }
                            }
                            CounterForwardMsg::WsFrame(_) => {}
                        }
                    }
                    while let Some(cmd) = aeron_cmd_rx.pop() {
                        did_work = true;
                        if queue_metrics {
                            metric_drained += 1;
                        }
                        match cmd {
                            AeronCmd::NewOrder(req) => {
                                // if let Some(ref t) = spin_tracer { t.record_sym(MS_CMD_RING_POPPED, req.client_order_id, &req.symbol); }
                                let sym = std::str::from_utf8(&req.symbol)
                                    .unwrap_or("")
                                    .trim_end_matches('\0');
                                let sym_rules = lightning_exchange::symbol_rules::SymbolRules::for_symbol(sym);
                                let prelim_freeze = if req.side == 0 { sym_rules.ticks_to_price(req.price_ticks) } else { 0.0 };
                                remember_runtime_order(
                                    &order_meta_cache,
                                    req.client_order_id,
                                    req.participant_id as i64,
                                    prelim_freeze,
                                    sym_rules.lots_to_quantity(req.quantity_lots),
                                    req.side,
                                    req.symbol,
                                    0, // overwritten on ACCEPTED with authoritative value from pending_meta
                                );
                                if let Some(pub_) = order_pubs.get_mut(sym) {
                                    let _ = pub_.publish_new_order(&req);
                                } else {
                                    let coid: u64 = req.client_order_id;
                                    let uid: u64 = req.participant_id;
                                    pending_meta.remove(&coid);
                                    remove_runtime_order(&order_meta_cache, coid, coid);
                                    if let Some(tx) = user_tx.get(uid as i64) {
                                        let _ = tx.try_send((lightning_exchange::ws_sbe::encode_order_rejected(
                                            0, &format!("No engine for symbol: {}", sym),
                                        ), 0));
                                    }
                                }
                                // if let Some(ref t) = spin_tracer { t.record_sym(MS_AERON_ORDER_SEND, req.client_order_id, &req.symbol); }
                            }
                            AeronCmd::Cancel(req) => {
                                let symbol_bytes =
                                    runtime_meta(&order_meta_cache, req.order_id).map(|m| m.symbol);
                                let routed = if let Some(sym_bytes) = symbol_bytes {
                                    let sym_end =
                                        sym_bytes.iter().position(|&b| b == 0).unwrap_or(16);
                                    let sym =
                                        std::str::from_utf8(&sym_bytes[..sym_end]).unwrap_or("");
                                    if let Some(pub_) = order_pubs.get_mut(sym) {
                                        let _ = pub_.publish_cancel(&req);
                                        true
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                };
                                if !routed {
                                    for pub_ in order_pubs.values_mut() {
                                        let _ = pub_.publish_cancel(&req);
                                    }
                                }
                            }
                            AeronCmd::BatchCancel(reqs) => {
                                for req in &reqs {
                                    let symbol_bytes =
                                        runtime_meta(&order_meta_cache, req.order_id)
                                            .map(|m| m.symbol);
                                    let routed = if let Some(sym_bytes) = symbol_bytes {
                                        let sym_end =
                                            sym_bytes.iter().position(|&b| b == 0).unwrap_or(16);
                                        let sym = std::str::from_utf8(&sym_bytes[..sym_end])
                                            .unwrap_or("");
                                        if let Some(pub_) = order_pubs.get_mut(sym) {
                                            let _ = pub_.publish_cancel(req);
                                            true
                                        } else {
                                            false
                                        }
                                    } else {
                                        false
                                    };
                                    if !routed {
                                        for pub_ in order_pubs.values_mut() {
                                            let _ = pub_.publish_cancel(req);
                                        }
                                    }
                                }
                            }
                            AeronCmd::BatchNewOrder(reqs) => {
                                for req in &reqs {
                                    let sym = std::str::from_utf8(&req.symbol)
                                        .unwrap_or("")
                                        .trim_end_matches('\0');
                                    let sym_rules = lightning_exchange::symbol_rules::SymbolRules::for_symbol(sym);
                                    let prelim_freeze = if req.side == 0 { sym_rules.ticks_to_price(req.price_ticks) } else { 0.0 };
                                    remember_runtime_order(
                                        &order_meta_cache,
                                        req.client_order_id,
                                        req.participant_id as i64,
                                        prelim_freeze,
                                        sym_rules.lots_to_quantity(req.quantity_lots),
                                        req.side,
                                        req.symbol,
                                        0, // overwritten on ACCEPTED with authoritative value from pending_meta
                                    );
                                    if let Some(pub_) = order_pubs.get_mut(sym) {
                                        let _ = pub_.publish_new_order(req);
                                    }
                                    // if let Some(ref t) = spin_tracer { t.record_sym(MS_AERON_ORDER_SEND, req.client_order_id, &req.symbol); }
                                }
                            }
                        }
                    }
                    if !did_work {
                        if use_spin {
                            std::hint::spin_loop();
                        } else {
                            idle_us = (idle_us * 2 + 10).min(500);
                            std::thread::sleep(std::time::Duration::from_micros(idle_us));
                        }
                    } else {
                        idle_us = 0;
                    }
                    if queue_metrics && metric_last.elapsed() >= std::time::Duration::from_secs(1) {
                        let len = aeron_cmd_rx.len();
                        tracing::warn!(
                            "aeron_cmd queue: len={} max_len={} drained_per_s={}",
                            len,
                            metric_max_len,
                            metric_drained,
                        );
                        metric_last = std::time::Instant::now();
                        metric_drained = 0;
                        metric_max_len = len;
                    }
                }
            })?;
    }

    // ── Aeron RECV "public" thread: trade + depth (broadcast market data) ────
    // Public market data (trade tape + depth snapshots) is independent of
    // private order updates. OKX/Binance split these onto separate WS
    // endpoints precisely because public data tolerates ms-scale latency
    // while private order acks need <1ms. Running them on the SAME thread
    // means processing a 256-msg depth burst stalls OrderUpdate routing
    // and pumps Aeron-outbound transit from <100µs to ~300µs.
    if let Some((mut trade_sub, mut depth_sub)) = public_market_data_subs {
        let market_fanout = state.market_fanout.clone();
        let account_cache_pub = account_cache.clone();
        let user_tx_pub = state.user_tx.clone();
        let last_depth = state.last_depth.clone();
        let last_trade_price = state.last_trade_price.clone();
        let last_ticker = state.last_ticker.clone();
        let persist_pub_pub = persist_pub.clone();
        let vwap_cache_pub = vwap_cache.clone();
        let rt_pub = tokio::runtime::Handle::current();
        let order_meta_cache_pub = open_order_meta.clone();
        let risk_engine_pub = state.risk_engine.clone();
        let exchange_config_pub = state.exchange_config.clone();
        let index_agg_pub = index_agg.clone();
        let mark_frozen_pub = mark_frozen_count.clone();
        let last_mark_update_pub = last_mark_update.clone();
        let mark_clamp_bps: i64 = std::env::var("MARK_CLAMP_BPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);
        let thread_name_recv_pub = format!("d{desk_id}-recv-pub");
        std::thread::Builder::new()
            .name(thread_name_recv_pub.clone())
            .spawn(move || {
                pin_current_thread_to_core("DESK_RECV_PUBLIC_CORE", &thread_name_recv_pub);
                let mut live_market_data = LiveMarketData::default();
                let mut settle_batch: [SettleTradeEntry; 64] = [SettleTradeEntry {
                    taker_id: 0, maker_id: 0, taker_uid: 0, maker_uid: 0,
                    price: 0.0, qty: 0.0, side: 0, symbol: [0; 16],
                }; 64];
                let mut settle_count: usize = 0;
                let mut idle_us: u64 = 0;
                loop {
                    let mut did_work = false;
                    trade_sub.do_work();
                    depth_sub.do_work();

                    while let Some(trade) = trade_sub.poll() {
                        did_work = true;
                        let price: f64 = trade.price;
                        let qty: f64 = trade.quantity;
                        let side: u8 = trade.side;
                        let taker_id = trade.taker_order_id as i64;
                        let maker_id = trade.maker_order_id as i64;
                        let taker_uid =
                            runtime_user_id(&order_meta_cache_pub, taker_id as u64);
                        let maker_uid =
                            runtime_user_id(&order_meta_cache_pub, maker_id as u64);
                        tracing::info!(
                            "trade consumed: taker_id={taker_id} maker_id={maker_id}                              taker_uid={taker_uid} maker_uid={maker_uid} qty={qty}"
                        );
                        let mut sym = [0u8; 16];
                        sym.copy_from_slice(&trade.symbol[..16]);
                        let sym_end = sym.iter().position(|&b| b == 0).unwrap_or(16);
                        let symbol_str =
                            std::str::from_utf8(&sym[..sym_end]).unwrap_or("BTC_USDT");
                        let ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as u64)
                            .unwrap_or(0);
                        let rules =
                            lightning_exchange::desk::symbol_rules::SymbolRules::for_symbol(
                                symbol_str,
                            );
                        let price_ticks = rules
                            .price_to_ticks(price)
                            .unwrap_or_else(|_| (price / rules.price_tick).round() as i64);
                        let quantity_lots = rules
                            .quantity_to_lots(qty)
                            .unwrap_or_else(|_| (qty / rules.quantity_step).round() as i64);
                        publish_frame(
                            &persist_pub_pub,
                            &PersistFrame::matching_event(MatchingEventPayload {
                                sequence: trade.sequence,
                                response_stream_id: TRADE_STREAM,
                                event_kind: matching_event_kind::TRADE,
                                _pad: [0; 3],
                                order_id: taker_id,
                                client_order_id: 0,
                                participant_id: taker_uid,
                                counterparty_order_id: maker_id,
                                symbol: pack_str(symbol_str),
                                price_ticks,
                                quantity_lots,
                                remaining_lots: 0,
                                ts_ns: ts * 1_000,
                            }),
                        );
                        market_fanout.send_owned(lightning_exchange::ws_sbe::encode_trade(
                            price, qty, side, ts, symbol_str,
                        ));
                        last_trade_price.insert(symbol_str.to_string(), price);
                        {
                            let sbe_msgs =
                                live_market_data.ingest(symbol_str, ts, price, qty);
                            // Store ticker SBE for REST; REST decodes on demand.
                            last_ticker.insert(symbol_str.to_string(), Arc::from(sbe_msgs[0].as_slice()));
                            if market_fanout.subscriber_count() != 0 {
                                for sbe in sbe_msgs {
                                    market_fanout.send_owned(sbe);
                                }
                            }
                        }
                        if settle_count >= 64 {
                            process_db_cmd(
                                DbCmd::BatchSettleTrade {
                                    entries: settle_batch,
                                    count: settle_count as u8,
                                },
                                &account_cache_pub,
                                &user_tx_pub,
                                &persist_pub_pub,
                                &vwap_cache_pub,
                                &risk_engine_pub,
                                &exchange_config_pub,
                            );
                            settle_count = 0;
                        }
                        settle_batch[settle_count] = SettleTradeEntry {
                            taker_id, maker_id, taker_uid, maker_uid,
                            price, qty, side, symbol: sym,
                        };
                        settle_count += 1;
                    }
                    if settle_count > 0 {
                        process_db_cmd(
                            DbCmd::BatchSettleTrade {
                                entries: settle_batch,
                                count: settle_count as u8,
                            },
                            &account_cache_pub,
                            &user_tx_pub,
                            &persist_pub_pub,
                            &vwap_cache_pub,
                            &risk_engine_pub,
                            &exchange_config_pub,
                        );
                        settle_count = 0;
                    }

                    while let Some(depth_msg) = depth_sub.poll() {
                        did_work = true;
                        use lightning_exchange::aeron_transport::DeskDepthMsg;
                        match depth_msg {
                            DeskDepthMsg::Depth(evt) => {
                                let nb = evt.num_bids as usize;
                                let na = evt.num_asks as usize;
                                let mut bids_raw = [(0.0f64, 0.0f64); 20];
                                let mut asks_raw = [(0.0f64, 0.0f64); 20];
                                bids_raw[..nb].copy_from_slice(&evt.bids[..nb]);
                                asks_raw[..na].copy_from_slice(&evt.asks[..na]);
                                let end = evt.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                                let symbol = std::str::from_utf8(&evt.symbol[..end])
                                    .unwrap_or("BTC_USDT").to_string();
                                let symbol = if symbol.is_empty() {
                                    "BTC_USDT".to_string()
                                } else { symbol };
                                if nb == 0 || na == 0 { continue; }

                                // Update EWMA mark price from mid = (best_bid + best_ask) / 2.
                                {
                                    let sym_key = evt.symbol;
                                    let best_bid = bids_raw[..nb].iter().map(|(p, _)| *p).fold(f64::NEG_INFINITY, f64::max);
                                    let best_ask = asks_raw[..na].iter().map(|(p, _)| *p).fold(f64::INFINITY, f64::min);
                                    if best_bid > 0.0 && best_ask < f64::INFINITY {
                                        let sym_str_end = sym_key.iter().position(|&b| b == 0).unwrap_or(16);
                                        let sym_str = std::str::from_utf8(&sym_key[..sym_str_end]).unwrap_or("");
                                        let rules = lightning_exchange::desk::symbol_rules::SymbolRules::for_symbol(sym_str);
                                        let mid_ticks = ((best_bid + best_ask) / 2.0 / rules.price_tick).round() as i64;
                                        // S4.2: the mark is the mid CLAMPED into
                                        // index × (1 ± MARK_CLAMP_BPS). With the
                                        // index frozen (sources below quorum) the
                                        // mark is NOT updated at all — the engine
                                        // keeps its last value rather than trust
                                        // a manipulable book (counted in metrics).
                                        match index_agg_pub.as_ref().map(|a| {
                                            use lightning_exchange::desk::funding::IndexPriceSource;
                                            a.index_price_ticks(&sym_key)
                                        }) {
                                            Some(Some(index_ticks)) => {
                                                let mark = lightning_exchange::desk::index_price::clamped_mark(
                                                    mid_ticks, index_ticks, mark_clamp_bps,
                                                );
                                                risk_engine_pub.update_mark_price(sym_key, mark, rules.notional_scale);
                                                // B5: record that the mark was updated now.
                                                last_mark_update_pub.insert(sym_key, std::time::Instant::now());
                                            }
                                            Some(None) => {
                                                mark_frozen_pub.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            }
                                            // No aggregator configured (INDEX_SOURCES
                                            // empty): raw mid, pre-S4 behavior — for
                                            // dev/test setups without external feeds.
                                            None => {
                                                risk_engine_pub.update_mark_price(sym_key, mid_ticks, rules.notional_scale);
                                                // B5: record that the mark was updated now.
                                                last_mark_update_pub.insert(sym_key, std::time::Instant::now());
                                            }
                                        }
                                    }
                                }

                                let mf2 = market_fanout.clone();
                                let last_depth2 = last_depth.clone();
                                rt_pub.spawn(async move {
                                    let bids: Vec<(f64, f64)> = bids_raw[..nb]
                                        .iter().filter(|(_, q)| *q > 0.0)
                                        .map(|&(p, q)| (p, q)).collect();
                                    let asks: Vec<(f64, f64)> = asks_raw[..na]
                                        .iter().filter(|(_, q)| *q > 0.0)
                                        .map(|&(p, q)| (p, q)).collect();
                                    let ts = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .map(|d| d.as_micros() as u64).unwrap_or(0);
                                    // Store as SBE — REST decodes on demand, WS gets bytes directly.
                                    let sbe = lightning_exchange::ws_sbe::encode_depth(ts, &symbol, &bids, &asks);
                                    last_depth2.insert(symbol.clone(), Arc::from(sbe.as_slice()));
                                    mf2.send_owned(sbe);
                                });
                            }
                            DeskDepthMsg::Depth50(_) | DeskDepthMsg::Level2(_) => {}
                        }
                    }

                    if !did_work {
                        // Public data is ms-scale tolerant — sleep when idle
                        // instead of spinning a core. Wakeup latency 50-200µs
                        // is fine for trade/depth fan-out.
                        idle_us = (idle_us * 2 + 10).min(500);
                        std::thread::sleep(std::time::Duration::from_micros(idle_us));
                    } else {
                        idle_us = 0;
                    }
                }
            })?;
    }

    // ── Aeron RECV "private" spin: OrderUpdate only (latency-critical) ───────
    {
        let market_fanout = state.market_fanout.clone();
        let pending_orders = state.pending_orders.clone();
        let pending_meta = state.pending_meta.clone();
        let user_tx = state.user_tx.clone();
        let account_cache = account_cache.clone();
        let last_depth = state.last_depth.clone();
        let last_trade_price = state.last_trade_price.clone();
        let last_ticker = state.last_ticker.clone();
        let persist_pub = persist_pub.clone();
        let vwap_cache = vwap_cache.clone();
        let rt = tokio::runtime::Handle::current();
        let spin_tracer = tracer.clone();
        let order_meta_cache = open_order_meta.clone();
        let forward_origin_recv = forwarded_order_origin.clone();
        let public_to_engine_recv = forwarded_public_to_engine.clone();
        let engine_to_public_recv = forwarded_engine_to_public.clone();
        let recv_cf_resp_pubs: HashMap<u16, CounterForwardPublisher> = (0..COUNTER_SHARD_COUNT)
            .map(|desk| {
                let stream = counter_forward_resp_stream_for_desk(desk);
                let pub_ = new_counter_forward_publisher_with_retry(
                    counter_forward_resp_client.clone(),
                    &cf_channel,
                    stream,
                    "recv-resp",
                );
                (desk, pub_)
            })
            .collect();
        let risk_engine = state.risk_engine.clone();
        let exchange_config_recv = state.exchange_config.clone();

        let thread_name_recv = format!("d{desk_id}-recv");
        std::thread::Builder::new()
            .name(thread_name_recv.clone())
            .spawn(move || {
                pin_current_thread_to_core("DESK_RECV_CORE", &thread_name_recv);
                let mut idle_us: u64 = 0;
                let counter_forward_response_failures = AtomicU64::new(0);
                let mut counter_forward_resp_pubs = recv_cf_resp_pubs;
                // Accumulator for CANCELLED events observed in one poll burst.
                // PR5b: each entry carries the fully-resolved (user_id,
                // asset, release_amount), looked up via OrderRuntimeMeta
                // when CANCELED arrives. DB worker becomes pure cache-mutate
                // + publish — no PG SELECT/UPDATE.
                let mut cancel_batch: [CancelReleaseEntry; 64] = [CancelReleaseEntry {
                    id: 0, user_id: 0, asset: [0; 16], release_amount: 0.0,
                }; 64];
                let mut cancel_count: usize = 0;

                // Same pattern for ACCEPTED — flushed as one
                // DbCmd::BatchUpsertOrder (~11× faster than per-id INSERT;
                // see examples/bench_upsert_order).
                let mut accepted_batch: [OrderInsertEntry; 64] = [OrderInsertEntry {
                    id: 0, user_id: 0, symbol: [0; 16], side: 0,
                    order_type: [0; 16], price: 0.0, qty: 0.0, filled: 0.0,
                    status: 0, freeze_price: 0.0, do_freeze: false,
                    client_order_id: [0; 32],
                }; 64];
                let mut accepted_count: usize = 0;

                // FILLED / REJECTED rows deleted in one DELETE…ANY per burst
                // (~9× faster locally; see examples/bench_delete_order).
                let mut delete_batch: [i64; 64] = [0; 64];
                let mut delete_count: usize = 0;
                // Hot-path warning counters: rate-limited to once per 10K.
                // At 400K conns the unrate-limited warns were costing ~1.7 s
                // of CPU per test inside the recv-spin critical section.
                let lost_order_updates = AtomicU64::new(0);
                let full_channels = AtomicU64::new(0);
                let mut last_order_update_epoch: u16 = 0;
                    let mut fenced_drop_count: u64 = 0;
                    let m_fenced =
                        lightning_exchange::metrics::counter("desk_fenced_drops_total");
                    let m_gaps =
                        lightning_exchange::metrics::counter("desk_order_update_gaps_total");
                    let mut last_order_update_seq: u64 = 0;
                let mut order_update_gap_count: u64 = 0;
                loop {
                    let mut did_work = false;

                    order_update_sub.do_work();
                    counter_forward_resp_sub.do_work();
                    // trade/depth poll moved to aeron-recv-public to keep this
                    // thread laser-focused on private order updates.

                    while let Some(msg) = counter_forward_resp_sub.poll() {
                        did_work = true;
                        if let CounterForwardMsg::WsFrame(frame) = msg {
                            let user_id = frame.user_id;
                            let public_order_id = frame.order_id;
                            let len = frame.len;
                            counter_forward_debug!(
                                "cf_ingress_ws_frame desk={} user={} public_order={} len={}",
                                desk_id,
                                user_id,
                                public_order_id,
                                len
                            );
                            if let Some(tx) = user_tx.get(user_id) {
                                let _ = tx.try_send((frame.payload().to_vec(), public_order_id));
                            }
                        }
                    }

                    // Process order updates — complete pending REST/WS requests.
                    while let Some(msg) = order_update_sub.poll() {
                        did_work = true;
                        use lightning_exchange::transport::order_update_kind;
                        // Copy packed struct fields to locals to avoid misaligned refs.
                        let sequence: u64 = msg.sequence;
                        if sequence != 0 {
                            // Fencing: high 16 bits carry the leader epoch.
                            // Output from an epoch BELOW the highest seen is
                            // a zombie ex-leader — drop the message entirely.
                            let (epoch, seq) =
                                lightning_exchange::leader::split_epoch(sequence);
                            if epoch < last_order_update_epoch {
                                fenced_drop_count += 1;
                                m_fenced.inc();
                                if fenced_drop_count == 1 || fenced_drop_count % 1024 == 0 {
                                    tracing::warn!(
                                        "FENCED: dropped order_update from stale epoch {} (current {}), total {}",
                                        epoch, last_order_update_epoch, fenced_drop_count
                                    );
                                }
                                continue;
                            }
                            if epoch > last_order_update_epoch {
                                tracing::info!(
                                    "leader epoch {} → {} (failover); sequence tracking reset",
                                    last_order_update_epoch, epoch
                                );
                                last_order_update_epoch = epoch;
                                last_order_update_seq = 0;
                            }
                            let expected = last_order_update_seq.saturating_add(1);
                            if seq != expected {
                                order_update_gap_count += 1;
                                m_gaps.inc();
                                if order_update_gap_count == 1
                                    || order_update_gap_count % 1024 == 0
                                {
                                    tracing::warn!(
                                        "order_update sequence gap desk={} expected={} got={} gaps={}",
                                        desk_id,
                                        expected,
                                        seq,
                                        order_update_gap_count
                                    );
                                }
                            }
                            last_order_update_seq = seq;
                        }
                        let order_id: u64 = msg.order_id;
                        let client_order_id: u64 = msg.client_order_id;
                        let participant_id: u64 = msg.participant_id;
                        let fill_qty: f64 = msg.fill_qty;
                        let fill_price: f64 = msg.fill_price;
                        let remaining_qty: f64 = msg.remaining_qty;
                        let update_ts: u64 = msg.timestamp;
                        let kind: u8 = msg.kind;
                        // Owner-filter: every desk subscribes to OrderUpdate via
                        // Aeron fan-out, so without this check each desk re-runs
                        // user_tx routing + persist publish + account_cache
                        // mutate for ALL orders — 4 desks = 4× duplicate work
                        // and 4× write amplification into pg-writer/redis-writer.
                        //
                        // Skip if neither (a) the user has a live WS connection
                        // on this desk, nor (b) a REST request is awaiting this
                        // order_id locally, nor (c) this desk originated the
                        // order (pending_meta still holds client_order_id).
                        // Any one being true means "this is my order".
                        let lookup_id_for_pending = if kind == order_update_kind::REJECTED {
                            client_order_id
                        } else {
                            order_id
                        };
                        let owned = user_tx.get(participant_id as i64).is_some()
                            || pending_orders.contains_key(&lookup_id_for_pending)
                            || pending_meta.contains_key(&lookup_id_for_pending)
                            || pending_meta.contains_key(&client_order_id)
                            || forward_origin_recv.contains_key(&lookup_id_for_pending)
                            || forward_origin_recv.contains_key(&client_order_id);
                        if !owned {
                            continue;
                        }
                        // if let Some(ref t) = spin_tracer { t.record(MS_AERON_UPDATE_RECV, client_order_id); }
                        // For WS fast-path orders, pending_meta holds the order
                        // details. On ACCEPTED we INSERT the DB row + freeze funds.
                        // On REJECTED/CANCELLED we just drop the meta (no freeze happened).
                        // Cache user_id synchronously before removing from pending_meta
                        // so the trade handler can resolve UIDs without any DB round-trip.
                        //
                        // REJECTED messages have order_id=0 (engine never assigned one);
                        // use client_order_id (= desk's internal order_id) for the lookup.
                        let lookup_id = if kind == order_update_kind::REJECTED { client_order_id } else { order_id };
                        // Route to waiting REST request (pending_orders) if any.
                        if let Some((_, tx)) = pending_orders.remove(&lookup_id) {
                            let _ = tx.send(msg);
                        }
                        if kind == order_update_kind::ACCEPTED {
                            remap_runtime_order_id(
                                &order_meta_cache,
                                client_order_id,
                                order_id,
                            );
                            if let Some((_, ingress)) = forward_origin_recv.remove(&client_order_id) {
                                forward_origin_recv.insert(order_id, ingress);
                                public_to_engine_recv.insert(client_order_id, order_id);
                                engine_to_public_recv.insert(order_id, client_order_id);
                            }
                        }
                        let ws_meta = pending_meta
                            .remove(&lookup_id)
                            .or_else(|| {
                                if lookup_id == client_order_id {
                                    None
                                } else {
                                    pending_meta.remove(&client_order_id)
                                }
                            })
                            .map(|(_, m)| m);
                        if let Some(meta_ref) = ws_meta.as_ref() {
                            if kind != order_update_kind::REJECTED {
                                // Pending_meta has the authoritative freeze_price
                                // (best_opposing for markets, limit price for buys,
                                // 0 for sells). Overwrite the preliminary value
                                // stored at NewOrder/BatchNewOrder time so cancel
                                // releases use the right amount. side/symbol/qty
                                // unchanged.
                                remember_runtime_order(
                                    &order_meta_cache,
                                    order_id,
                                    meta_ref.user_id,
                                    meta_ref.freeze_price,
                                    meta_ref.qty,
                                    meta_ref.side,
                                    meta_ref.symbol,
                                    meta_ref.initial_margin_atoms,
                                );
                                // remember_runtime_order always zeros liq_price_ticks;
                                // restore it so on_fill settles the user at the correct price.
                                if meta_ref.liq_price_ticks != 0 {
                                    if let Some(mut entry) = order_meta_cache.get_mut(&order_id) {
                                        entry.liq_price_ticks = meta_ref.liq_price_ticks;
                                    }
                                }
                            }
                        }
                        // client_order_id is only available on the first event (ACCEPTED).
                        let ws_client_oid = ws_meta.as_ref().map(|m| m.client_order_id.as_str());
                        if let Some(meta) = ws_meta
                            .as_ref()
                            .map(|m| OrderRuntimeMeta {
                                user_id: m.user_id,
                                freeze_price: m.freeze_price,
                                quantity: m.qty,
                                filled: 0.0,
                                side: m.side,
                                symbol: m.symbol,
                                initial_margin_atoms: m.initial_margin_atoms,
                                liq_price_ticks: m.liq_price_ticks,
                            })
                            .or_else(|| runtime_meta(&order_meta_cache, order_id))
                        {
                            publish_matching_order_event(
                                &persist_pub,
                                sequence,
                                response_stream_id,
                                kind,
                                order_id,
                                client_order_id,
                                participant_id,
                                fill_price,
                                fill_qty,
                                remaining_qty,
                                update_ts,
                                &meta,
                            );
                        }
                        let mut update_pushed = false;

                        // For the client's place-order SLA, ACCEPTED is the
                        // critical update. Send it before cache/persist batch
                        // work below so persistence cannot sit in front of the
                        // WS notification on the recv spin thread.
                        if kind == order_update_kind::ACCEPTED {
                            // if let Some(ref t) = spin_tracer { t.record(MS_WS_UPDATE_SEND, client_order_id); }
                            let ts = msg.timestamp;
                            let public_order_id = engine_to_public_recv
                                .get(&order_id)
                                .map(|e| *e.value())
                                .unwrap_or(order_id);
                            let upd = lightning_exchange::ws_sbe::encode_order_update(
                                public_order_id, 0, lightning_exchange::ws_sbe::WS_STATUS_OPEN,
                                fill_qty, fill_price, ts,
                            );
                            if let Some(ingress) = forward_origin_recv
                                .get(&order_id)
                                .map(|e| *e.value())
                                .or_else(|| forward_origin_recv.get(&client_order_id).map(|e| *e.value()))
                            {
                                counter_forward_debug!(
                                    "cf_owner_accept desk={} ingress={} user={} engine_order={} public_order={}",
                                    desk_id,
                                    ingress,
                                    participant_id,
                                    order_id,
                                    public_order_id
                                );
                                if let Some(frame) =
                                    CounterForwardWsFrame::new(participant_id as i64, public_order_id, &upd)
                                {
                                    if let Some(pub_) = counter_forward_resp_pubs.get_mut(&ingress) {
                                        if pub_.publish_ws_frame(&frame).is_err() {
                                            log_counter_forward_publish_failure(
                                                &counter_forward_response_failures,
                                                "owner_accept_to_ingress",
                                            );
                                        }
                                    }
                                }
                            }
                            if let Some(tx) = user_tx.get(participant_id as i64) {
                                if tx.try_send((upd, client_order_id)).is_err() {
                                    let n = full_channels.fetch_add(1, Ordering::Relaxed) + 1;
                                    if n % 10_000 == 0 {
                                        tracing::warn!("personal channel full — order_update dropped (total: {n})");
                                    }
                                }
                                // if let Some(ref t) = spin_tracer { t.record(MS_USER_TX_SENT, client_order_id); }
                            }
                            update_pushed = true;
                        }

                        if let Some(meta) = ws_meta.as_ref() {
                            if kind == order_update_kind::ACCEPTED {
                                // Accumulate into the batched-INSERT buffer.
                                // Flushed below as a single DbCmd::BatchUpsertOrder
                                // after the poll burst ends (~11× faster than
                                // per-id UpsertOrder; see bench_upsert_order).
                                if accepted_count >= 64 {
                                    process_db_cmd(DbCmd::BatchUpsertOrder {
                                            entries: accepted_batch,
                                            count: accepted_count as u8,
                                        }, &account_cache, &user_tx, &persist_pub, &vwap_cache, &risk_engine, &exchange_config_recv);
                                    accepted_count = 0;
                                }
                                accepted_batch[accepted_count] = OrderInsertEntry {
                                    id:              order_id as i64,
                                    user_id:         meta.user_id,
                                    symbol:          meta.symbol,
                                    side:            meta.side,
                                    order_type:      meta.order_type,
                                    price:           meta.price.unwrap_or(0.0),
                                    qty:             meta.qty,
                                    filled:          0.0,
                                    status:          DbOrderStatus::Pending.as_u8(),
                                    freeze_price:    meta.freeze_price,
                                    do_freeze:       true,
                                    client_order_id: db_cmd::str_bytes(ws_client_oid.unwrap_or("")),
                                };
                                accepted_count += 1;
                            } else if kind == order_update_kind::PARTIAL_FILL {
                                // Order is still resting — UpsertOrder writes the live row.
                                process_db_cmd(DbCmd::UpsertOrder {
                                    id:              order_id as i64,
                                    user_id:         meta.user_id,
                                    symbol:          meta.symbol,
                                    side:            meta.side,
                                    order_type:      meta.order_type,
                                    price:           meta.price.unwrap_or(0.0),
                                    qty:             meta.qty,
                                    filled:          fill_qty,
                                    status:          DbOrderStatus::Trading.as_u8(),
                                    freeze_price:    meta.freeze_price,
                                    do_freeze:       false,
                                    client_order_id: db_cmd::str_bytes(ws_client_oid.unwrap_or("")),
                                }, &account_cache, &user_tx, &persist_pub, &vwap_cache, &risk_engine, &exchange_config_recv);
                            }
                            // kind == FILLED here means "first event was a full fill"
                            // (market / IOC). Skip INSERT — the order is already terminal,
                            // and trades has no FK to orders so settle is unaffected.
                            if kind == order_update_kind::REJECTED {
                                // Freeze was in-memory only (hot path never hit DB).
                                // Revert account_cache and risk_engine; no DB write needed.
                                let symbol = unpack_str16(&meta.symbol).unwrap_or("BTC_USDT");
                                let (_, quote) = symbol.split_once('_').unwrap_or(("BTC", "USDT"));
                                let rel_amount = if meta.initial_margin_atoms > 0 {
                                    meta.initial_margin_atoms as f64 / 100.0
                                } else if meta.side == 0 {
                                    meta.freeze_price * meta.qty
                                } else {
                                    meta.qty
                                };
                                let asset = if meta.side == 0 || meta.initial_margin_atoms > 0 {
                                    quote
                                } else {
                                    symbol.split_once('_').map(|(b, _)| b).unwrap_or("BTC")
                                };
                                if meta.initial_margin_atoms > 0 {
                                    risk_engine.release_order_margin(meta.user_id, meta.initial_margin_atoms);
                                }
                                // If this was a liquidation order that got rejected,
                                // unblock the account so run_risk_tick can retry.
                                if meta.liq_price_ticks != 0 {
                                    risk_engine.set_account_status_if(
                                        meta.user_id,
                                        lightning_exchange::desk::risk::RiskStatus::Liquidating,
                                        lightning_exchange::desk::risk::RiskStatus::LiquidationPending,
                                    );
                                }
                                let new_vals = account_cache.get_mut(&meta.user_id).and_then(|mut e| {
                                    let kv = e.get_mut(asset)?;
                                    let rel_atoms = AmountAtoms::from_f64_round(rel_amount).ok()?;
                                    kv.release_atoms(rel_atoms.atoms());
                                    Some(*kv)
                                });
                                if let Some(snapshot) = new_vals {
                                    let bal = snapshot.balance();
                                    let frz = snapshot.frozen();
                                    if let Some(tx) = user_tx.get(meta.user_id) {
                                        let _ = tx.try_send((lightning_exchange::ws_sbe::encode_balance_update(
                                            asset, bal, bal - frz, frz,
                                        ), 0));
                                    }
                                }
                            } else if kind == order_update_kind::CANCELLED {
                                if meta.initial_margin_atoms > 0 {
                                    risk_engine.release_order_margin(meta.user_id, meta.initial_margin_atoms);
                                }
                                // B1: if a liquidation IOC was CANCELLED (e.g. no
                                // liquidity), re-arm the account to LiquidationPending
                                // so run_risk_tick retries on the next tick.
                                if meta.liq_price_ticks != 0 {
                                    risk_engine.set_account_status_if(
                                        meta.user_id,
                                        lightning_exchange::desk::risk::RiskStatus::Liquidating,
                                        lightning_exchange::desk::risk::RiskStatus::LiquidationPending,
                                    );
                                }
                                process_db_cmd(DbCmd::ReleaseReservation {
                                    user_id: meta.user_id,
                                    symbol: meta.symbol,
                                    side: meta.side,
                                    qty: meta.qty,
                                    freeze_price: meta.freeze_price,
                                }, &account_cache, &user_tx, &persist_pub, &vwap_cache, &risk_engine, &exchange_config_recv);
                            }
                        } else {
                            // REST-path order OR subsequent WS update (row already exists).
                            // Terminal states get dropped from the orders table; only
                            // PARTIAL_FILL (still resting) updates filled/status.
                            if kind == order_update_kind::CANCELLED {
                                // PR5b: resolve (user_id, asset, release_amount)
                                // from runtime meta — was a PG SELECT in the DB
                                // worker before. Skip the entry if we have no
                                // meta (e.g. order placed before this desk-server
                                // restart and not yet rehydrated) — falling
                                // through is safe; the order simply won't have
                                // funds released by this path, but cache state
                                // remains correct because no freeze happened on
                                // our side either.
                                //
                                // Release futures margin reserved at order placement.
                                // This path handles GTC ACCEPTED→CANCELLED: pending_meta
                                // was consumed on ACCEPTED so ws_meta is None above.
                                if let Some(m) = runtime_meta(&order_meta_cache, order_id) {
                                    if m.initial_margin_atoms > 0 {
                                        risk_engine.release_order_margin(m.user_id, m.initial_margin_atoms);
                                    }
                                }
                                let entry = runtime_meta(&order_meta_cache, order_id)
                                    .and_then(|m| {
                                        let release_qty = (m.quantity - m.filled).max(0.0);
                                        if release_qty <= 0.0 {
                                            return Some(CancelReleaseEntry {
                                                id: order_id as i64,
                                                user_id: m.user_id,
                                                asset: [0; 16],
                                                release_amount: 0.0,
                                            });
                                        }
                                        let sym_end = m.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                                        let symbol = std::str::from_utf8(&m.symbol[..sym_end]).ok()?;
                                        let (base, quote) = symbol.split_once('_')?;
                                        let (asset_str, amount) = if m.side == 1 {
                                            // sell: release base by remaining qty
                                            (base, release_qty)
                                        } else {
                                            // buy: release quote by freeze_price * remaining qty
                                            (quote, m.freeze_price * release_qty)
                                        };
                                        let mut asset_bytes = [0u8; 16];
                                        let ab = asset_str.as_bytes();
                                        let n = ab.len().min(16);
                                        asset_bytes[..n].copy_from_slice(&ab[..n]);
                                        Some(CancelReleaseEntry {
                                            id: order_id as i64,
                                            user_id: m.user_id,
                                            asset: asset_bytes,
                                            release_amount: amount,
                                        })
                                    })
                                    .unwrap_or(CancelReleaseEntry {
                                        id: order_id as i64,
                                        user_id: 0,
                                        asset: [0; 16],
                                        release_amount: 0.0,
                                    });
                                if cancel_count >= 64 {
                                    process_db_cmd(DbCmd::BatchCancelConfirmed {
                                            entries: cancel_batch,
                                            count: cancel_count as u8,
                                        }, &account_cache, &user_tx, &persist_pub, &vwap_cache, &risk_engine, &exchange_config_recv);
                                    cancel_count = 0;
                                }
                                cancel_batch[cancel_count] = entry;
                                cancel_count += 1;
                            } else if kind == order_update_kind::FILLED
                                || kind == order_update_kind::REJECTED
                            {
                                // Batched DELETE — flushed after the burst.
                                if delete_count >= 64 {
                                    process_db_cmd(DbCmd::BatchDeleteOrder {
                                            ids: delete_batch,
                                            count: delete_count as u8,
                                        }, &account_cache, &user_tx, &persist_pub, &vwap_cache, &risk_engine, &exchange_config_recv);
                                    delete_count = 0;
                                }
                                delete_batch[delete_count] = order_id as i64;
                                delete_count += 1;
                            } else {
                                let status = db_status_from_update_kind(kind).as_u8();
                                process_db_cmd(DbCmd::UpdateStatus {
                                    id:     order_id as i64,
                                    status,
                                    filled: fill_qty,
                                }, &account_cache, &user_tx, &persist_pub, &vwap_cache, &risk_engine, &exchange_config_recv);
                            }
                        }

                        if !update_pushed {
                            // Push order_update to user's personal WS channel.
                            let user_id = participant_id as i64;
                            // if let Some(ref t) = spin_tracer { t.record(MS_WS_UPDATE_SEND, client_order_id); }
                            // Single DashMap lookup — was previously two (is_none() +
                            // if let Some()), each taking a shard lock.
                            let maybe_tx = user_tx.get(user_id);
                            if maybe_tx.is_none() {
                                // Rate-limited counter — at 400K conns this used to
                                // hit ~175K times/test, each `tracing::warn!` is
                                // ~10µs format + stderr lock → ~1.7s of pure log
                                // time on the recv-spin's critical path. Now we
                                // count and only print every 10K.
                                let n = lost_order_updates.fetch_add(1, Ordering::Relaxed) + 1;
                                if n % 10_000 == 0 {
                                    tracing::warn!("no WS channel — order_update lost (total: {n})");
                                }
                            }
                            if let Some(tx) = maybe_tx {
                                let ws_status = ws_status_from_update_kind(kind);
                                let status_byte = lightning_exchange::ws_sbe::ws_status_byte_from_str(ws_status.as_str());
                                let ts = msg.timestamp;
                                let upd = lightning_exchange::ws_sbe::encode_order_update(
                                    order_id, 0, status_byte, fill_qty, fill_price, ts,
                                );
                                if let Some(ingress) = forward_origin_recv
                                    .get(&order_id)
                                    .map(|e| *e.value())
                                    .or_else(|| forward_origin_recv.get(&client_order_id).map(|e| *e.value()))
                                {
                                    let public_order_id = engine_to_public_recv
                                        .get(&order_id)
                                        .map(|e| *e.value())
                                        .unwrap_or(client_order_id);
                                    let upd = lightning_exchange::ws_sbe::encode_order_update(
                                        public_order_id, 0, status_byte, fill_qty, fill_price, ts,
                                    );
                                    if let Some(frame) =
                                        CounterForwardWsFrame::new(user_id, public_order_id, &upd)
                                    {
                                        if let Some(pub_) = counter_forward_resp_pubs.get_mut(&ingress) {
                                            if pub_.publish_ws_frame(&frame).is_err() {
                                                log_counter_forward_publish_failure(
                                                    &counter_forward_response_failures,
                                                    "owner_update_to_ingress_local_tx",
                                                );
                                            }
                                        }
                                    }
                                }
                                if tx.try_send((upd, 0)).is_err() {
                                    let n = full_channels.fetch_add(1, Ordering::Relaxed) + 1;
                                    if n % 10_000 == 0 {
                                        tracing::warn!("personal channel full — order_update dropped (total: {n})");
                                    }
                                }
                            } else if let Some(ingress) = forward_origin_recv
                                .get(&order_id)
                                .map(|e| *e.value())
                                .or_else(|| forward_origin_recv.get(&client_order_id).map(|e| *e.value()))
                            {
                                let ws_status = ws_status_from_update_kind(kind);
                                let status_byte = lightning_exchange::ws_sbe::ws_status_byte_from_str(ws_status.as_str());
                                let ts = msg.timestamp;
                                let public_order_id = engine_to_public_recv
                                    .get(&order_id)
                                    .map(|e| *e.value())
                                    .unwrap_or(client_order_id);
                                let upd = lightning_exchange::ws_sbe::encode_order_update(
                                    public_order_id, 0, status_byte, fill_qty, fill_price, ts,
                                );
                                if let Some(frame) =
                                    CounterForwardWsFrame::new(user_id, public_order_id, &upd)
                                {
                                    if let Some(pub_) = counter_forward_resp_pubs.get_mut(&ingress) {
                                        if pub_.publish_ws_frame(&frame).is_err() {
                                            log_counter_forward_publish_failure(
                                                &counter_forward_response_failures,
                                                "owner_update_to_ingress_no_local_tx",
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        // Phase 2: update position state on every fill.
                        if kind == order_update_kind::FILLED
                            || kind == order_update_kind::PARTIAL_FILL
                        {
                            if let Some(meta) = runtime_meta(&order_meta_cache, order_id) {
                                let sym_end =
                                    meta.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                                let sym_str = std::str::from_utf8(&meta.symbol[..sym_end])
                                    .unwrap_or("BTC_USDT");
                                let rules = lightning_exchange::symbol_rules::SymbolRules::for_symbol(sym_str);
                                let fp_ticks = (fill_price / rules.price_tick).round() as i64;
                                let fq_lots = rules.quantity_to_lots(fill_qty).unwrap_or(0);
                                if fq_lots > 0 {
                                    let notional =
                                        lightning_exchange::desk::risk::calc::calc_notional_atoms(
                                            fp_ticks, fq_lots, rules.notional_scale,
                                        );
                                    // Liquidation orders have no reserved order_margin (the close
                                    // side has no margin requirement). Pass 0 so on_fill does not
                                    // deduct from order_margin that was never reserved.
                                    if meta.liq_price_ticks != 0 {
                                        if let Some(ref t) = spin_tracer {
                                            t.record(MS_LIQ_FILL_RECV, order_id);
                                        }
                                    }
                                    let fill_margin = if meta.liq_price_ticks != 0 {
                                        0
                                    } else {
                                        lightning_exchange::desk::risk::calc::calc_initial_margin_atoms(
                                            notional, rules.default_leverage,
                                        )
                                    };
                                    risk_engine.on_fill(
                                        meta.user_id,
                                        meta.symbol,
                                        meta.side,
                                        fp_ticks,
                                        fq_lots,
                                        fill_margin,
                                        rules.notional_scale,
                                        rules.default_leverage,
                                        rules.maintenance_rate_bps,
                                        meta.liq_price_ticks,
                                    );
                                    // S7: taker fee (liquidation orders pay
                                    // none — their spread already funds the
                                    // exchange).
                                    if meta.liq_price_ticks == 0 {
                                        let (_, taker_bps) = exchange_config_recv.fee_bps(sym_str);
                                        let fill_notional =
                                            lightning_exchange::desk::risk::calc::calc_notional_atoms(
                                                fp_ticks, fq_lots, rules.notional_scale,
                                            );
                                        risk_engine.charge_fee(
                                            meta.user_id,
                                            lightning_exchange::desk::risk::calc::fee_atoms(
                                                fill_notional, taker_bps,
                                            ),
                                        );
                                    }
                                    // S1.3: persist the margin state this fill
                                    // produced. Absolute-state frames (position
                                    // row, account row, insurance fund) ride the
                                    // same journaled persist stream as orders/
                                    // trades — pg-writer applies them in the
                                    // same exactly-once transaction.
                                    for frame in
                                        lightning_exchange::desk::risk_persist::margin_state_frames(
                                            &risk_engine,
                                            meta.user_id,
                                            &meta.symbol,
                                        )
                                    {
                                        publish_frame(&persist_pub, &frame);
                                    }
                                }
                            }
                        }

                        if kind == order_update_kind::FILLED
                            || kind == order_update_kind::CANCELLED
                            || kind == order_update_kind::REJECTED
                        {
                            forward_origin_recv.remove(&order_id);
                            forward_origin_recv.remove(&client_order_id);
                            if let Some((_, public_order_id)) = engine_to_public_recv.remove(&order_id) {
                                public_to_engine_recv.remove(&public_order_id);
                            }
                            if let Some((_, engine_order_id)) = public_to_engine_recv.remove(&client_order_id) {
                                engine_to_public_recv.remove(&engine_order_id);
                            }
                            remove_runtime_order(&order_meta_cache, order_id, client_order_id);
                        }
                    }

                    // Settle batch lives on aeron-recv-public — nothing to flush here.

                    // Flush per-burst ACCEPTED accumulator as one batched
                    // multi-row INSERT + grouped freeze UPDATE. Cuts work for
                    // an MM 20-id place cycle from 40 round-trips (2 per id)
                    // to ~3 total.
                    if accepted_count > 0 {
                        process_db_cmd(DbCmd::BatchUpsertOrder {
                                entries: accepted_batch,
                                count: accepted_count as u8,
                            }, &account_cache, &user_tx, &persist_pub, &vwap_cache, &risk_engine, &exchange_config_recv);
                        accepted_count = 0;
                    }

                    // Flush per-burst FILLED/REJECTED DELETE accumulator.
                    // ~9× faster than per-id DELETE (examples/bench_delete_order).
                    if delete_count > 0 {
                        process_db_cmd(DbCmd::BatchDeleteOrder {
                                ids: delete_batch,
                                count: delete_count as u8,
                            }, &account_cache, &user_tx, &persist_pub, &vwap_cache, &risk_engine, &exchange_config_recv);
                        delete_count = 0;
                    }

                    // Flush the per-burst CANCELLED accumulator as one batched
                    // DB cmd. Cuts DB work for an MM 20-id cancel cycle from
                    // 60 round-trips (3 per id) to ~3 total.
                    if cancel_count > 0 {
                        process_db_cmd(DbCmd::BatchCancelConfirmed {
                                entries: cancel_batch,
                                count: cancel_count as u8,
                            }, &account_cache, &user_tx, &persist_pub, &vwap_cache, &risk_engine, &exchange_config_recv);
                        cancel_count = 0;
                    }

                    // depth poll moved to aeron-recv-public.

                    if !did_work {
                        if use_spin {
                            std::hint::spin_loop();
                        } else {
                            idle_us = (idle_us * 2 + 10).min(500);
                            std::thread::sleep(std::time::Duration::from_micros(idle_us));
                        }
                    } else {
                        idle_us = 0;
                    }
                }
            })?;
    }

    // ── 10ms risk tick: mark-to-market account status check + liquidation ────
    {
        let risk_engine_tick = state.risk_engine.clone();
        let next_order_id_tick = state.next_order_id.clone();
        let pending_meta_tick = state.pending_meta.clone();
        let liq_cmd_tick = state.liq_cmd_tx.clone();
        let persist_pub_tick = persist_pub.clone();
        let symbols_tick: Vec<String> = std::env::var("SYMBOLS")
            .unwrap_or_else(|_| "ETH_USDT,BTC_USDT,SOL_USDT".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let tracer_liq = state.tracer.clone();
        // B4: skip liquidation when no real index price source is configured.
        let index_sources_empty_tick = index_sources_empty;
        // B5: last mark timestamp per symbol (written by the public spin thread).
        let last_mark_update_tick = last_mark_update.clone();
        let mark_freeze_threshold_tick = std::time::Duration::from_secs(mark_freeze_threshold_secs);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                // S6.2 — ADL sweep: clear any bankrupt position whose
                // liquidation IOC couldn't fill (rare; cold path).
                for sym in symbols_tick.iter() {
                    let sym16 = lightning_exchange::transport::persist_event::pack_str(sym);
                    let rules =
                        lightning_exchange::desk::symbol_rules::SymbolRules::for_symbol(sym);
                    let adl = risk_engine_tick.run_adl(
                        sym16,
                        rules.notional_scale,
                        rules.default_leverage,
                        rules.maintenance_rate_bps,
                    );
                    for ev in adl {
                        tracing::warn!(
                            "ADL: bankrupt {} deleveraged against {} ({} lots @ {} ticks)",
                            ev.bankrupt_user_id,
                            ev.counterparty_user_id,
                            ev.qty_lots,
                            ev.price_ticks
                        );
                        for uid in [ev.bankrupt_user_id, ev.counterparty_user_id] {
                            for frame in
                                lightning_exchange::desk::risk_persist::margin_state_frames(
                                    &risk_engine_tick,
                                    uid,
                                    &sym16,
                                )
                            {
                                publish_frame(&persist_pub_tick, &frame);
                            }
                        }
                    }
                }
                let to_liquidate = risk_engine_tick.run_risk_tick();
                // S6.1 — tiered liquidation: close LIQ_TRANCHE_BPS of the
                // position per round (default 10_000 = full, pre-S6
                // behavior). The partial close resets the account to
                // Normal; the next tick re-evaluates and emits the next
                // tranche until the account is healthy or flat.
                let tranche_bps: i64 = std::env::var("LIQ_TRANCHE_BPS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(10_000);
                for evt in &to_liquidate {
                    if let Some(ref t) = tracer_liq {
                        t.record_sym(MS_LIQ_TICK_EMIT, evt.user_id as u64, &evt.symbol);
                    }
                }
                for evt in to_liquidate {
                    use lightning_exchange::desk::risk::PositionSide;
                    use lightning_exchange::sbe::NewOrderRequest as SbeNewOrder;
                    use lightning_exchange::transport::{AeronCmd, OrderMeta, pack_str16};
                    use std::sync::atomic::Ordering;

                    // Guard: zero liq_price_ticks means position data is stale — skip
                    // rather than sending a zero-price order that would be rejected and
                    // leave the account permanently stuck in Liquidating.
                    if evt.liq_price_ticks == 0 {
                        tracing::warn!(
                            user_id = evt.user_id,
                            "liquidation skipped: zero liq_price_ticks"
                        );
                        continue;
                    }

                    // B4: no real index price source — raw mid is untrustworthy for
                    // liquidation decisions; skip until INDEX_SOURCES is configured.
                    if index_sources_empty_tick {
                        tracing::warn!(
                            user_id = evt.user_id,
                            "liquidation skipped: INDEX_SOURCES is empty, mark price unreliable"
                        );
                        continue;
                    }

                    // B5: stale mark price — skip liquidation for this symbol to avoid
                    // triggering at an outdated price.
                    {
                        let sym_str_end = evt.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                        let sym_str_b5 =
                            std::str::from_utf8(&evt.symbol[..sym_str_end]).unwrap_or("?");
                        let is_stale = match last_mark_update_tick.get(&evt.symbol) {
                            Some(last) => last.elapsed() > mark_freeze_threshold_tick,
                            None => true,
                        };
                        if is_stale {
                            tracing::warn!(
                                user_id = evt.user_id,
                                symbol = sym_str_b5,
                                "mark price frozen for symbol -- skipping liquidation check"
                            );
                            continue;
                        }
                    }

                    // Mark account as Liquidating immediately so no new orders can be placed.
                    if let Some(mut acct) = risk_engine_tick.accounts.get_mut(&evt.user_id) {
                        acct.status = lightning_exchange::desk::risk::RiskStatus::Liquidating;
                    }

                    let Some(ref cmd_tx) = liq_cmd_tick else {
                        continue;
                    };

                    let sym_str_end = evt.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                    let sym_str =
                        std::str::from_utf8(&evt.symbol[..sym_str_end]).unwrap_or("BTC_USDT");
                    let rules =
                        lightning_exchange::desk::symbol_rules::SymbolRules::for_symbol(sym_str);

                    // Liquidation side is opposite to position side.
                    let liq_side: u8 = if evt.side == PositionSide::Long { 1 } else { 0 };
                    let order_id = next_order_id_tick.fetch_add(1, Ordering::Relaxed);

                    // Liquidation order is a limit IOC at the liquidation price.
                    // This is deliberately aggressive (well into the book), so it
                    // fills at market while the user is settled at liq_price_ticks.
                    let tranche_lots =
                        lightning_exchange::desk::risk::calc::liquidation_tranche_lots(
                            evt.qty_lots,
                            tranche_bps,
                        );
                    let sbe_req = SbeNewOrder {
                        client_order_id: order_id,
                        participant_id: evt.user_id as u64,
                        price_ticks: evt.liq_price_ticks,
                        quantity_lots: tranche_lots,
                        side: liq_side,
                        time_in_force: 1, // IOC
                        response_stream_id,
                        reduce_only: 0,
                        _pad: [0; 9],
                        symbol: evt.symbol,
                    };

                    // Notional and margin for fill accounting on the close.
                    let mark_ticks = risk_engine_tick.mark_price_ticks(&evt.symbol).unwrap_or(0);
                    let notional = if mark_ticks > 0 {
                        lightning_exchange::desk::risk::calc::calc_notional_atoms(
                            mark_ticks,
                            tranche_lots,
                            rules.notional_scale,
                        )
                    } else {
                        0
                    };
                    let margin_atoms =
                        lightning_exchange::desk::risk::calc::calc_initial_margin_atoms(
                            notional,
                            rules.default_leverage,
                        );

                    // Register metadata so on_fill can settle user at liq_price_ticks
                    // and credit the spread to the insurance fund.
                    let order_type = if liq_side == 0 { "liq-buy" } else { "liq-sell" };
                    pending_meta_tick.insert(
                        order_id,
                        OrderMeta {
                            user_id: evt.user_id,
                            symbol: evt.symbol,
                            side: liq_side,
                            order_type: pack_str16(order_type),
                            price: None,
                            qty: rules.lots_to_quantity(tranche_lots),
                            client_order_id: format!("liq-{}", order_id),
                            freeze_price: 0.0,
                            initial_margin_atoms: margin_atoms,
                            liq_price_ticks: evt.liq_price_ticks,
                        },
                    );

                    if cmd_tx.push(AeronCmd::NewOrder(sbe_req)).is_err() {
                        pending_meta_tick.remove(&order_id);
                        tracing::warn!(
                            user_id = evt.user_id,
                            "liquidation order dropped: ring full"
                        );
                    } else {
                        if let Some(ref t) = tracer_liq {
                            t.record_sym(MS_LIQ_ORDER_SENT, order_id, &evt.symbol);
                        }
                        tracing::warn!(
                            user_id = evt.user_id,
                            order_id,
                            qty_lots = evt.qty_lots,
                            sym = sym_str,
                            "liquidation order sent"
                        );
                    }
                }
            }
        });
    }

    // ── Periodic depth broadcaster ───────────────────────────────────────────
    // Live ticker/kline/agg is generated from Aeron trade events. This
    // periodic task is off by default and only handles depth snapshots.
    // ── S5: trigger hydrate + recovery + firing task ───────────────────
    {
        use lightning_exchange::desk::trigger::{
            PendingTrigger, TriggerWhen, firing_margin_atoms, needs_reinjection,
        };
        use lightning_exchange::transport::{AeronCmd, OrderMeta, pack_str16};
        let pool_t = state.db.clone();
        let runtime_meta_t = open_order_meta.clone();
        let books = trigger_books.clone();
        let engine_t = state.risk_engine.clone();
        let cmd_tx_t = state.liq_cmd_tx.clone();
        let next_id_t = state.next_order_id.clone();
        let pending_meta_t = state.pending_meta.clone();
        let resp_stream_t = state.response_stream_id;
        tokio::spawn(async move {
            // Hydrate pending triggers.
            let rows: Vec<(i64, i64, String, String, String, i64, String, Option<i64>, i64)> =
                sqlx::query_as(
                    "SELECT id, user_id, symbol, side, order_type, trigger_price_ticks,
                            trigger_when, price_ticks, qty_lots
                       FROM trigger_orders WHERE status = 'pending'",
                )
                .fetch_all(pool_t.as_ref())
                .await
                .unwrap_or_default();
            let hydrated = rows.len();
            for (id, user_id, symbol, side_s, ot, trig, when_s, price, qty) in rows {
                let Some(when) = TriggerWhen::from_db_str(&when_s) else { continue };
                books.entry(symbol).or_default().lock().insert(
                    when,
                    PendingTrigger {
                        id,
                        user_id,
                        side: if side_s == "buy" { 0 } else { 1 },
                        is_market: ot == "market",
                        trigger_price_ticks: trig,
                        price_ticks: price,
                        qty_lots: qty,
                    },
                );
            }
            // Recovery: 'triggered' rows whose injected order left NO
            // footprint (desk died between the status flip and the
            // publish) — re-inject with the SAME pre-allocated id.
            let orphans: Vec<(i64, i64, i64, String, String, Option<i64>, i64, String)> =
                sqlx::query_as(
                    r#"SELECT t.id, t.triggered_order_id, t.user_id, t.symbol, t.side,
                              t.price_ticks, t.qty_lots, t.order_type
                         FROM trigger_orders t
                        WHERE t.status = 'triggered'
                          AND t.triggered_order_id IS NOT NULL
                          AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.id = t.triggered_order_id)
                          AND NOT EXISTS (SELECT 1 FROM matching_events m
                                           WHERE m.order_id = t.triggered_order_id)"#,
                )
                .fetch_all(pool_t.as_ref())
                .await
                .unwrap_or_default();
            tracing::info!(
                "trigger hydrate: {} pending, {} interrupted firing(s) to recover",
                hydrated,
                orphans.len()
            );
            for (tid, oid, user_id, symbol, side_s, price, qty, ot) in orphans {
                debug_assert!(needs_reinjection(false, false));
                let side: u8 = if side_s == "buy" { 0 } else { 1 };
                inject_trigger_order(
                    &pool_t, &engine_t, &cmd_tx_t, &pending_meta_t, &runtime_meta_t,
                    resp_stream_t, tid, oid, user_id, &symbol, side, ot == "market",
                    price, qty, /*already_marked=*/ true,
                )
                .await;
            }

            // Firing loop: 50ms mark polling per symbol book.
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(50));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                for entry in books.iter() {
                    let symbol = entry.key().clone();
                    let sym16 = lightning_exchange::transport::persist_event::pack_str(&symbol);
                    let Some(mark) = engine_t.mark_price_ticks(&sym16) else { continue };
                    let fired = {
                        let mut book = entry.value().lock();
                        if book.is_empty() {
                            continue;
                        }
                        book.due(mark)
                    };
                    for t in fired {
                        let oid = next_id_t
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            as i64;
                        inject_trigger_order(
                            &pool_t, &engine_t, &cmd_tx_t, &pending_meta_t, &runtime_meta_t,
                            resp_stream_t, t.id, oid, t.user_id, &symbol, t.side,
                            t.is_market, t.price_ticks, t.qty_lots,
                            /*already_marked=*/ false,
                        )
                        .await;
                    }
                }
            }
        });
    }

    let market_broadcaster_enabled = std::env::var("MARKET_DATA_BROADCASTER")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);
    if market_broadcaster_enabled {
        tokio::spawn(market_data_broadcaster(state.clone()));
    }

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = router(state).layer(cors);

    let addr_str = format!("0.0.0.0:{}", port);
    tracing::info!("Desk Server listening on {}", addr_str);

    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as ConnBuilder;
    use tower_service::Service;

    let listener = TcpListener::bind(&addr_str).await?;

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok((s, p)) => {
                let _ = s.set_nodelay(true);
                (s, p)
            }
            Err(e) => {
                tracing::warn!("accept error: {e}");
                continue;
            }
        };

        let app_per_conn = app.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let builder = ConnBuilder::new(TokioExecutor::new());
            let svc_fn = hyper::service::service_fn(move |req| {
                let mut svc = app_per_conn.clone();
                async move { svc.call(req).await }
            });
            let conn = builder.serve_connection_with_upgrades(io, svc_fn);
            if let Err(e) = conn.await {
                tracing::debug!("conn from {peer} ended: {e}");
            }
        });
    }
}
