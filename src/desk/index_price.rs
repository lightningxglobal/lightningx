//! Index price aggregation + mark-price clamping — S4.
//!
//! Why this exists: liquidations and funding key off the MARK price. If
//! the mark is just our own order book's mid, anyone who can sweep a thin
//! book can move it ±20% for one tick and harvest every liquidation it
//! triggers. The defense, standard across perp venues:
//!
//!   index = median over INDEPENDENT external sources (outliers dropped),
//!   mark  = book mid CLAMPED into index × (1 ± α).
//!
//! Failure posture: with fewer than `min_sources` fresh sources the
//! aggregator returns None and the desk FREEZES the mark (the engine
//! keeps its last value) rather than falling back to the manipulable
//! mid — no liquidations on made-up prices. Frozen time is observable
//! (health counters → /metrics) and alarms per the runbook.
//!
//! Sources are intentionally dumb: anything (the generic HTTP pollers in
//! [`spawn_http_pollers`], a test, a future websocket feed) just stores
//! (price_ticks, now_ms) into a [`SharedSource`]; the aggregator only
//! ever READS. No locks on the hot path — DashMap point reads + atomics.

use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// One external price source; writers store, the aggregator reads.
pub struct SharedSource {
    pub name: String,
    /// symbol → (price_ticks, updated_at_ms).
    prices: DashMap<[u8; 16], (i64, i64)>,
}

impl SharedSource {
    pub fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            prices: DashMap::new(),
        })
    }

    pub fn store(&self, symbol: [u8; 16], price_ticks: i64, now_ms: i64) {
        if price_ticks > 0 {
            self.prices.insert(symbol, (price_ticks, now_ms));
        }
    }

    fn fresh(&self, symbol: &[u8; 16], now_ms: i64, staleness_ms: i64) -> Option<i64> {
        let (price, ts) = *self.prices.get(symbol)?;
        (now_ms - ts <= staleness_ms).then_some(price)
    }
}

/// Aggregator health counters (exported as metrics by the desk).
#[derive(Default)]
pub struct IndexHealth {
    /// Aggregations that produced an index.
    pub ok: AtomicU64,
    /// Aggregations frozen for lack of fresh sources.
    pub frozen: AtomicU64,
    /// Individual source prices dropped as outliers.
    pub outliers_dropped: AtomicU64,
}

pub struct IndexAggregator {
    sources: Vec<Arc<SharedSource>>,
    /// A source older than this is ignored (default 30s).
    pub staleness_ms: i64,
    /// A source deviating more than this from the median is dropped
    /// (default 200 bps = 2%).
    pub outlier_bps: i64,
    /// Fewer fresh sources than this → None (freeze; default 2).
    pub min_sources: usize,
    pub health: IndexHealth,
}

impl IndexAggregator {
    pub fn new(sources: Vec<Arc<SharedSource>>) -> Self {
        let get = |k: &str, d: i64| -> i64 {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        };
        Self {
            sources,
            staleness_ms: get("INDEX_STALENESS_MS", 30_000),
            outlier_bps: get("INDEX_OUTLIER_BPS", 200),
            min_sources: get("INDEX_MIN_SOURCES", 2) as usize,
            health: IndexHealth::default(),
        }
    }

    /// Median of fresh sources after one round of outlier rejection.
    /// `now_ms` is injected (testability; callers pass wall clock).
    pub fn index_at(&self, symbol: &[u8; 16], now_ms: i64) -> Option<i64> {
        let mut prices: Vec<i64> = self
            .sources
            .iter()
            .filter_map(|s| s.fresh(symbol, now_ms, self.staleness_ms))
            .collect();
        if prices.len() < self.min_sources {
            self.health.frozen.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        prices.sort_unstable();
        let median = prices[prices.len() / 2];

        // One rejection round: drop sources too far from the first
        // median, then re-median the survivors. A single compromised
        // source can therefore never move the index by more than the
        // gap between honest sources.
        let survivors: Vec<i64> = prices
            .iter()
            .copied()
            .filter(|p| {
                let dev_bps = ((p - median).abs() as i128 * 10_000) / median.max(1) as i128;
                let keep = dev_bps <= self.outlier_bps as i128;
                if !keep {
                    self.health.outliers_dropped.fetch_add(1, Ordering::Relaxed);
                }
                keep
            })
            .collect();
        if survivors.len() < self.min_sources {
            self.health.frozen.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        self.health.ok.fetch_add(1, Ordering::Relaxed);
        Some(survivors[survivors.len() / 2])
    }
}

impl crate::desk::funding::IndexPriceSource for IndexAggregator {
    fn index_price_ticks(&self, symbol: &[u8; 16]) -> Option<i64> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.index_at(symbol, now_ms)
    }
}

/// S4.2 — the mark-price rule: the book's mid, clamped into
/// index × (1 ± clamp_bps). Pure; the desk applies it at the SINGLE
/// choke point that feeds update_mark_price, so uPnL, maintenance,
/// liquidation triggers, liq order pricing and funding premium all
/// inherit the protection.
pub fn clamped_mark(mid_ticks: i64, index_ticks: i64, clamp_bps: i64) -> i64 {
    let lo = (index_ticks as i128 * (10_000 - clamp_bps) as i128 / 10_000) as i64;
    let hi = (index_ticks as i128 * (10_000 + clamp_bps) as i128 / 10_000) as i64;
    mid_ticks.clamp(lo, hi)
}

/// Generic HTTP pollers — S4.5. Config via env:
///   INDEX_SOURCES=binance,okx                      (names)
///   INDEX_SOURCE_BINANCE_URL=https://…?symbol={BASEQUOTE}
///   INDEX_SOURCE_BINANCE_PATH=/price               (JSON pointer)
///   INDEX_POLL_SECS=5
/// URL placeholders: {BASEQUOTE} → BTCUSDT, {BASE_QUOTE} → BTC_USDT,
/// {BASE-QUOTE} → BTC-USDT. The fetched f64 price is converted to ticks
/// with the symbol's rules. Poll failures only age the source out —
/// staleness handles the rest.
pub fn spawn_http_pollers(symbols: Vec<String>) -> Vec<Arc<SharedSource>> {
    let names: Vec<String> = std::env::var("INDEX_SOURCES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let poll_secs: u64 = std::env::var("INDEX_POLL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let mut out = Vec::new();
    for name in names {
        let upper = name.to_uppercase();
        let Ok(url_tpl) = std::env::var(format!("INDEX_SOURCE_{upper}_URL")) else {
            tracing::warn!("index source {name}: no URL configured — skipped");
            continue;
        };
        let path = std::env::var(format!("INDEX_SOURCE_{upper}_PATH"))
            .unwrap_or_else(|_| "/price".to_string());
        let source = SharedSource::new(name.clone());
        out.push(source.clone());
        let symbols = symbols.clone();
        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .expect("http client");
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(poll_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                for sym in &symbols {
                    let (base, quote) = sym.split_once('_').unwrap_or((sym.as_str(), ""));
                    let url = url_tpl
                        .replace("{BASEQUOTE}", &format!("{base}{quote}"))
                        .replace("{BASE_QUOTE}", sym)
                        .replace("{BASE-QUOTE}", &format!("{base}-{quote}"));
                    let Ok(resp) = http.get(&url).send().await else { continue };
                    let Ok(body) = resp.json::<serde_json::Value>().await else { continue };
                    let Some(price) = body
                        .pointer(&path)
                        .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
                    else {
                        continue;
                    };
                    let rules = crate::desk::symbol_rules::SymbolRules::for_symbol(sym);
                    let Ok(ticks) = rules.price_to_ticks(price) else { continue };
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    source.store(crate::transport::persist_event::pack_str(sym), ticks, now_ms);
                }
            }
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SYM: [u8; 16] = *b"IDX_TEST\0\0\0\0\0\0\0\0";

    fn agg(prices: &[(i64, i64)], staleness: i64, outlier_bps: i64, min: usize) -> IndexAggregator {
        let sources = prices
            .iter()
            .enumerate()
            .map(|(i, &(p, ts))| {
                let s = SharedSource::new(format!("s{i}"));
                s.store(SYM, p, ts);
                s
            })
            .collect();
        let mut a = IndexAggregator::new(sources);
        a.staleness_ms = staleness;
        a.outlier_bps = outlier_bps;
        a.min_sources = min;
        a
    }

    #[test]
    fn median_of_three_and_outlier_rejection() {
        // Honest 50_000/50_010, one compromised source at +30%.
        let a = agg(
            &[(5_000_000, 100), (5_001_000, 100), (6_500_000, 100)],
            1_000,
            200,
            2,
        );
        // Median = 5_001_000; 6_500_000 deviates ~30% > 2% → dropped;
        // re-median of the two survivors = 5_001_000.
        assert_eq!(a.index_at(&SYM, 100), Some(5_001_000));
        assert_eq!(a.health.outliers_dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn stale_sources_age_out_and_freeze() {
        let a = agg(&[(5_000_000, 0), (5_001_000, 0)], 1_000, 200, 2);
        assert!(a.index_at(&SYM, 500).is_some(), "fresh enough");
        assert_eq!(a.index_at(&SYM, 2_000), None, "all stale → freeze");
        assert!(a.health.frozen.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn below_min_sources_freezes() {
        let a = agg(&[(5_000_000, 100)], 1_000, 200, 2);
        assert_eq!(a.index_at(&SYM, 100), None, "1 < min_sources=2");
        // And if outlier rejection kills the quorum, freeze too.
        let a = agg(&[(5_000_000, 100), (9_000_000, 100)], 1_000, 200, 2);
        assert_eq!(a.index_at(&SYM, 100), None, "post-rejection quorum lost");
    }

    #[test]
    fn clamp_keeps_mark_inside_band() {
        let index = 5_000_000;
        // ±50 bps band: [4_975_000, 5_025_000].
        assert_eq!(clamped_mark(6_000_000, index, 50), 5_025_000, "pump capped");
        assert_eq!(clamped_mark(4_000_000, index, 50), 4_975_000, "dump capped");
        assert_eq!(clamped_mark(5_010_000, index, 50), 5_010_000, "honest mid passes");
    }
}
