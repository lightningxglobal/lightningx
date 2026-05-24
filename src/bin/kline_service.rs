//! K-line aggregation service.
//!
//! Subscribes to TradeNotification on Aeron TRADE_CHANNEL/TRADE_STREAM and
//! writes closed OHLCV bars (1m / 5m / 15m / 1h) to the `klines` table.

use aeron_wrapper::{AeronClient, NoopLifecycle, PollCallback};
use lightning_exchange::{
    aeron_channels::{AERON_DIR, TRADE_CHANNEL, TRADE_STREAM},
    sbe::TEMPLATE_TRADE_NOTIFICATION,
};
use parking_lot::Mutex;
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc;

// ─── Intervals ────────────────────────────────────────────────────────────────

const INTERVALS: &[(&str, u64)] = &[
    ("1m",  60),
    ("5m",  300),
    ("15m", 900),
    ("1h",  3600),
];

// ─── Bar ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Bar {
    symbol: String,
    interval: &'static str,
    interval_secs: u64,
    open_time: i64,   // Unix seconds, aligned to interval start
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    trade_count: u64,
}

impl Bar {
    fn new(
        symbol: String,
        interval: &'static str,
        interval_secs: u64,
        open_time: i64,
        price: f64,
        qty: f64,
    ) -> Self {
        Self {
            symbol,
            interval,
            interval_secs,
            open_time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: qty,
            trade_count: 1,
        }
    }

    fn update(&mut self, price: f64, qty: f64) {
        if price > self.high { self.high = price; }
        if price < self.low  { self.low  = price; }
        self.close = price;
        self.volume += qty;
        self.trade_count += 1;
    }

    fn close_time(&self) -> i64 {
        self.open_time + self.interval_secs as i64 - 1
    }
}

fn bar_epoch(ts_secs: i64, interval_secs: u64) -> i64 {
    (ts_secs / interval_secs as i64) * interval_secs as i64
}

// ─── Accumulator ──────────────────────────────────────────────────────────────

struct Accumulators {
    bars: HashMap<(String, &'static str), Bar>,
    closed_tx: mpsc::UnboundedSender<Bar>,
}

impl Accumulators {
    fn new(tx: mpsc::UnboundedSender<Bar>) -> Self {
        Self { bars: HashMap::new(), closed_tx: tx }
    }

    fn ingest(&mut self, symbol: &str, price: f64, qty: f64, ts_nanos: u64) {
        let ts_secs = (ts_nanos / 1_000_000_000) as i64;

        for &(interval, interval_secs) in INTERVALS {
            let epoch = bar_epoch(ts_secs, interval_secs);
            let key = (symbol.to_string(), interval);

            match self.bars.get_mut(&key) {
                Some(bar) if bar.open_time == epoch => {
                    bar.update(price, qty);
                }
                Some(_) => {
                    let closed = self.bars.remove(&key).unwrap();
                    let _ = self.closed_tx.send(closed);
                    let new_bar = Bar::new(
                        symbol.to_string(), interval, interval_secs, epoch, price, qty,
                    );
                    self.bars.insert((symbol.to_string(), interval), new_bar);
                }
                None => {
                    let bar = Bar::new(
                        symbol.to_string(), interval, interval_secs, epoch, price, qty,
                    );
                    self.bars.insert((symbol.to_string(), interval), bar);
                }
            }
        }
    }
}

// ─── Aeron callback ───────────────────────────────────────────────────────────

struct TradeCallback {
    // (symbol, price, qty, ts_nanos)
    queue: Arc<Mutex<VecDeque<(String, f64, f64, u64)>>>,
}

impl PollCallback for TradeCallback {
    fn on_data(&mut self, data: &[u8]) {
        // SBE wire format: 8-byte header + 64-byte TradeNotification = 72 bytes total
        if data.len() < 72 {
            return;
        }
        let template_id = u16::from_le_bytes([data[2], data[3]]);
        if template_id != TEMPLATE_TRADE_NOTIFICATION {
            return;
        }

        // Wire layout (all offsets from start of ):
        //   header: 0..8
        //   body:   8..72
        //     seq(8)=8..16  taker(8)=16..24  maker(8)=24..32
        //     price(8)=32..40  qty(8)=40..48  side(1)=48  pad(7)=49..56  symbol(16)=56..72
        let price = f64::from_le_bytes(data[32..40].try_into().unwrap());
        let quantity = f64::from_le_bytes(data[40..48].try_into().unwrap());
        let sym_raw = &data[56..72];
        let sym_len = sym_raw.iter().position(|&b| b == 0).unwrap_or(16);
        let symbol = match std::str::from_utf8(&sym_raw[..sym_len]) {
            Ok(s) if !s.is_empty() => s.to_string(),
            _ => return,
        };

        let ts_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        self.queue.lock().push_back((symbol, price, quantity, ts_nanos));
    }
}

// ─── DB writer ────────────────────────────────────────────────────────────────

async fn db_writer(pool: PgPool, mut rx: mpsc::UnboundedReceiver<Bar>) {
    while let Some(bar) = rx.recv().await {
        let result = sqlx::query(
            "INSERT INTO klines
               (symbol, interval, open_time, open, high, low, close, volume, close_time, trade_count)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (symbol, interval, open_time) DO UPDATE
             SET high        = GREATEST(klines.high, EXCLUDED.high),
                 low         = LEAST(klines.low, EXCLUDED.low),
                 close       = EXCLUDED.close,
                 volume      = klines.volume + EXCLUDED.volume,
                 trade_count = klines.trade_count + EXCLUDED.trade_count,
                 close_time  = EXCLUDED.close_time",
        )
        .bind(&bar.symbol)
        .bind(bar.interval)
        .bind(bar.open_time)
        .bind(bar.open)
        .bind(bar.high)
        .bind(bar.low)
        .bind(bar.close)
        .bind(bar.volume)
        .bind(bar.close_time())
        .bind(bar.trade_count as i64)
        .execute(&pool)
        .await;

        if let Err(e) = result {
            tracing::error!(
                "kline_service: upsert failed {}/{} open_time={}: {}",
                bar.symbol, bar.interval, bar.open_time, e
            );
        } else {
            tracing::debug!(
                "kline_service: wrote bar {}/{} open_time={}",
                bar.symbol, bar.interval, bar.open_time
            );
        }
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());

    tracing::info!("kline_service: connecting to database");
    let pool = lightning_exchange::db::create_pool(&database_url).await?;
    lightning_exchange::db::run_migrations(&pool).await?;
    tracing::info!("kline_service: migrations applied");

    let (closed_tx, closed_rx) = mpsc::unbounded_channel::<Bar>();

    tokio::spawn(db_writer(pool, closed_rx));

    // Shared queue: Aeron OS thread writes, tokio task drains
    let shared_queue: Arc<Mutex<VecDeque<(String, f64, f64, u64)>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let queue_for_aeron = shared_queue.clone();

    // tokio → accumulator channel
    let (trade_fwd_tx, mut trade_fwd_rx) = mpsc::unbounded_channel::<(String, f64, f64, u64)>();

    // Aeron poll loop — must run in a dedicated OS thread
    std::thread::spawn(move || {
        let client = match AeronClient::new(AERON_DIR) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                tracing::error!("kline_service: AeronClient::new failed: {:?}", e);
                return;
            }
        };

        let callback = TradeCallback { queue: queue_for_aeron.clone() };
        let mut sub = match client.add_subscription(
            TRADE_CHANNEL,
            TRADE_STREAM,
            1024,
            callback,
            NoopLifecycle,
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("kline_service: subscribe failed: {:?}", e);
                return;
            }
        };

        tracing::info!(
            "kline_service: subscribed to {} stream {}",
            TRADE_CHANNEL, TRADE_STREAM,
        );

        loop {
            client.do_work();
            let _ = sub.poll();

            // Drain the callback queue and forward trades to the tokio accumulator
            let mut batch = Vec::new();
            {
                let mut q = queue_for_aeron.lock();
                while let Some(item) = q.pop_front() {
                    batch.push(item);
                }
            }
            for item in batch {
                let _ = trade_fwd_tx.send(item);
            }

            std::hint::spin_loop();
        }
    });

    let mut accum = Accumulators::new(closed_tx);
    tracing::info!("kline_service: accumulator running");

    while let Some((symbol, price, qty, ts_nanos)) = trade_fwd_rx.recv().await {
        accum.ingest(&symbol, price, qty, ts_nanos);
    }

    Ok(())
}
