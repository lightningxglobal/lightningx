use axum::{
    extract::{Request, State},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use dashmap::DashMap;
use fastwebsockets::{upgrade, Frame, OpCode};
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use lightning_exchange::{
    aeron_channels::{
        aeron_dir, depth_channel, trade_channel, DEPTH50_STREAM, DEPTH_STREAM, LEVEL2_STREAM,
        TRADE_STREAM,
    },
    aeron_transport::{DeskDepthMsg, DeskDepthSubscriber, DeskTradeSubscriber},
    api::MarketFanout,
    write_actor::{WriteActorPool, WsCtrl},
    ws_sbe,
};
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use tokio::io as tio;
use tokio::sync::mpsc;
use tower_http::cors::{Any, CorsLayer};

#[derive(Clone)]
struct AppState {
    market_fanout: Arc<MarketFanout>,
    last_depth: Arc<DashMap<String, Arc<[u8]>>>,
    last_ticker: Arc<DashMap<String, Arc<[u8]>>>,
    write_pool: Arc<WriteActorPool>,
    read_pool: Arc<PublicReadActorPool>,
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
    fn ingest(&mut self, symbol: &str, ts_micros: u64, price: f64, qty: f64) -> [Vec<u8>; 4] {
        let ts_secs = (ts_micros / 1_000_000) as i64;
        let state = self.by_symbol.entry(symbol.to_string()).or_default();
        let (change, high, low, volume, kline, agg_1s, agg_5s) =
            state.ingest(ts_secs, price, qty);
        [
            ws_sbe::encode_ticker(symbol, price, change, high, low, volume),
            ws_sbe::encode_kline(
                symbol,
                1,
                kline.start as u64,
                kline.open,
                kline.high,
                kline.low,
                kline.close,
                kline.volume,
            ),
            ws_sbe::encode_agg_trade(
                symbol,
                0,
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
                0,
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
            let b = LiveBucket::new(start, price, qty);
            *bucket = Some(b);
            b
        }
    }
}

struct PublicConnInfo {
    personal_tx: mpsc::Sender<(Vec<u8>, u64)>,
    subscriptions: Arc<RwLock<HashSet<String>>>,
}

struct PublicReadConn {
    conn_id: u64,
    ws_read: fastwebsockets::WebSocketRead<tio::ReadHalf<TokioIo<Upgraded>>>,
    state: AppState,
    personal_tx: mpsc::Sender<(Vec<u8>, u64)>,
    ctrl_tx: mpsc::Sender<WsCtrl>,
    subscriptions: Arc<RwLock<HashSet<String>>>,
}

pub struct PublicReadActorPool {
    senders: Vec<mpsc::Sender<PublicReadConn>>,
    next: AtomicUsize,
    market_txs: Vec<mpsc::Sender<Arc<[u8]>>>,
}

impl PublicReadActorPool {
    fn new() -> Self {
        let n = read_actor_count();
        let mut senders = Vec::with_capacity(n);
        let mut market_txs = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel::<PublicReadConn>(1024);
            let (mtx, mrx) = mpsc::channel::<Arc<[u8]>>(128);
            tokio::spawn(public_read_actor_loop(rx, mrx));
            senders.push(tx);
            market_txs.push(mtx);
        }
        Self {
            senders,
            next: AtomicUsize::new(0),
            market_txs,
        }
    }

    fn market_senders(&self) -> Vec<mpsc::Sender<Arc<[u8]>>> {
        self.market_txs.clone()
    }

    fn register(&self, conn: PublicReadConn) {
        let n = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        let _ = self.senders[n].try_send(conn);
    }
}

fn read_actor_count() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("MARKET_READ_ACTORS")
            .or_else(|_| std::env::var("READ_ACTORS"))
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8)
    })
}

async fn public_read_actor_loop(
    mut conn_rx: mpsc::Receiver<PublicReadConn>,
    mut market_rx: mpsc::Receiver<Arc<[u8]>>,
) {
    let actor_conns: Arc<DashMap<u64, PublicConnInfo>> = Arc::new(DashMap::new());
    let actor_sub_count = Arc::new(AtomicUsize::new(0));
    let mut pending: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();

    loop {
        tokio::select! {
            msg = conn_rx.recv() => {
                match msg {
                    Some(c) => {
                        actor_conns.insert(c.conn_id, PublicConnInfo {
                            personal_tx: c.personal_tx.clone(),
                            subscriptions: c.subscriptions.clone(),
                        });
                        pending.push(Box::pin(public_read_conn_loop(
                            c,
                            actor_conns.clone(),
                            actor_sub_count.clone(),
                        )));
                    }
                    None => break,
                }
            }
            Some(_) = pending.next(), if !pending.is_empty() => {}
            Some(msg) = market_rx.recv() => {
                if actor_sub_count.load(Ordering::Relaxed) > 0 {
                    if let Some(channel) = market_channel(&msg) {
                        for entry in actor_conns.iter() {
                            let info = entry.value();
                            let should_send = info
                                .subscriptions
                                .read()
                                .map(|subs| subs.contains(&channel))
                                .unwrap_or(false);
                            if should_send {
                                let _ = info.personal_tx.try_send((msg.as_ref().to_owned(), 0));
                            }
                        }
                    }
                }
            }
        }
    }
    while pending.next().await.is_some() {}
}

async fn public_read_conn_loop(
    conn: PublicReadConn,
    actor_conns: Arc<DashMap<u64, PublicConnInfo>>,
    actor_sub_count: Arc<AtomicUsize>,
) {
    let PublicReadConn {
        conn_id,
        mut ws_read,
        state,
        personal_tx,
        ctrl_tx,
        subscriptions,
    } = conn;
    let mut noop_send =
        |_frame: Frame<'static>| std::future::ready(Ok::<(), fastwebsockets::WebSocketError>(()));
    let mut market_registered = false;

    loop {
        match ws_read.read_frame(&mut noop_send).await {
            Ok(frame) => match frame.opcode {
                OpCode::Text => {
                    let text = match std::str::from_utf8(&frame.payload) {
                        Ok(t) => t,
                        Err(_) => continue,
                    };
                    let msg = match PublicClientMsg::parse(text) {
                        Some(v) => v,
                        None => {
                            let _ = personal_tx.try_send((
                                ws_sbe::encode_error("Invalid market-data message format"),
                                0,
                            ));
                            continue;
                        }
                    };
                    handle_public_msg(
                        msg,
                        &state,
                        &personal_tx,
                        &subscriptions,
                        &actor_sub_count,
                        &mut market_registered,
                    );
                }
                OpCode::Ping => {
                    let _ = ctrl_tx.try_send(WsCtrl::Pong(frame.payload.to_vec()));
                }
                OpCode::Close => break,
                _ => {}
            },
            Err(_) => break,
        }
    }

    if market_registered {
        actor_sub_count.fetch_sub(1, Ordering::Relaxed);
        state.market_fanout.decrement_subscriber();
    }
    actor_conns.remove(&conn_id);
}

#[derive(Debug)]
enum PublicClientMsg {
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },
}

#[derive(Deserialize)]
struct PublicClientMsgWire {
    #[serde(rename = "type")]
    msg_type: String,
    channels: Option<Vec<String>>,
}

impl PublicClientMsg {
    fn parse(text: &str) -> Option<Self> {
        let wire: PublicClientMsgWire = serde_json::from_str(text).ok()?;
        match wire.msg_type.as_str() {
            "subscribe" => Some(Self::Subscribe {
                channels: wire.channels.unwrap_or_default(),
            }),
            "unsubscribe" => Some(Self::Unsubscribe {
                channels: wire.channels.unwrap_or_default(),
            }),
            _ => None,
        }
    }
}

fn handle_public_msg(
    msg: PublicClientMsg,
    state: &AppState,
    personal_tx: &mpsc::Sender<(Vec<u8>, u64)>,
    subscriptions: &Arc<RwLock<HashSet<String>>>,
    actor_sub_count: &AtomicUsize,
    market_registered: &mut bool,
) {
    let was_subscribed = subscriptions
        .read()
        .map(|subs| !subs.is_empty())
        .unwrap_or(false);

    match msg {
        PublicClientMsg::Subscribe { channels } => {
            let depth_symbols: Vec<String> = channels
                .iter()
                .filter_map(|c| c.strip_prefix("depth.").map(str::to_string))
                .collect();
            let ticker_symbols: Vec<String> = channels
                .iter()
                .filter_map(|c| c.strip_prefix("ticker.").map(str::to_string))
                .collect();
            if let Ok(mut subs) = subscriptions.write() {
                for ch in channels {
                    if is_public_channel(&ch) {
                        subs.insert(ch);
                    }
                }
            }
            for sym in depth_symbols {
                if let Some(depth_bytes) = state.last_depth.get(&sym) {
                    let _ = personal_tx.try_send((depth_bytes.to_vec(), 0));
                }
            }
            for sym in ticker_symbols {
                if let Some(ticker_bytes) = state.last_ticker.get(&sym) {
                    let _ = personal_tx.try_send((ticker_bytes.to_vec(), 0));
                }
            }
        }
        PublicClientMsg::Unsubscribe { channels } => {
            if let Ok(mut subs) = subscriptions.write() {
                for ch in channels {
                    subs.remove(&ch);
                }
            }
        }
    }

    let now_subscribed = subscriptions
        .read()
        .map(|subs| !subs.is_empty())
        .unwrap_or(false);
    if !was_subscribed && now_subscribed {
        actor_sub_count.fetch_add(1, Ordering::Relaxed);
        state.market_fanout.increment_subscriber();
        *market_registered = true;
    } else if was_subscribed && !now_subscribed {
        actor_sub_count.fetch_sub(1, Ordering::Relaxed);
        state.market_fanout.decrement_subscriber();
        *market_registered = false;
    }
}

fn is_public_channel(ch: &str) -> bool {
    ch.starts_with("depth.")
        || ch.starts_with("trades.")
        || ch.starts_with("ticker.")
        || ch.starts_with("kline.")
        || ch.starts_with("agg_trades.")
}

fn market_channel(msg: &[u8]) -> Option<String> {
    let prefix = match msg.first().copied()? {
        ws_sbe::DEPTH_MSG => "depth",
        ws_sbe::TRADE_MSG => "trades",
        ws_sbe::TICKER_MSG => "ticker",
        ws_sbe::KLINE_MSG => "kline",
        ws_sbe::AGG_TRADE_MSG => "agg_trades",
        _ => return None,
    };
    let symbol = ws_sbe::decode_broadcast_symbol(msg)?;
    Some(format!("{prefix}.{symbol}"))
}

async fn ws_handler(State(state): State<AppState>, mut req: Request) -> Response {
    let (response, fut) = match upgrade::upgrade(&mut req) {
        Ok(v) => v,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                format!("WS upgrade rejected: {e}"),
            )
                .into_response()
        }
    };
    tokio::spawn(async move {
        let Ok(mut socket) = fut.await else {
            return;
        };
        socket.set_writev(true);
        socket.set_auto_close(false);
        socket.set_auto_pong(false);

        static CONN_ID_GEN: AtomicU64 = AtomicU64::new(1);
        let conn_id = CONN_ID_GEN.fetch_add(1, Ordering::Relaxed);
        let (ws_read, ws_write) = socket.split(tio::split);
        let Some((personal_tx, ctrl_tx)) =
            state.write_pool.register(ws_write, market_ws_queue_cap(), None)
        else {
            return;
        };
        let subscriptions = Arc::new(RwLock::new(HashSet::new()));
        let read_pool = state.read_pool.clone();
        read_pool.register(PublicReadConn {
            conn_id,
            ws_read,
            state,
            personal_tx,
            ctrl_tx,
            subscriptions,
        });
    });
    response.map(|_| axum::body::Body::empty())
}

fn market_ws_queue_cap() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("MARKET_WS_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4096)
    })
}

fn unix_now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn spawn_aeron_public_loop(state: AppState) -> anyhow::Result<()> {
    let client = Arc::new(
        aeron_wrapper::AeronClient::new(&aeron_dir())
            .map_err(|e| anyhow::anyhow!("AeronClient: {:?}", e))?,
    );
    let mut trade_sub = DeskTradeSubscriber::new(client.clone(), &trade_channel(), TRADE_STREAM)
        .map_err(|e| anyhow::anyhow!("DeskTradeSubscriber: {}", e))?;
    let mut depth_sub = DeskDepthSubscriber::new(
        client,
        &depth_channel(),
        DEPTH_STREAM,
        DEPTH50_STREAM,
        LEVEL2_STREAM,
    )
    .map_err(|e| anyhow::anyhow!("DeskDepthSubscriber: {}", e))?;

    std::thread::Builder::new()
        .name("market-aeron-public".to_string())
        .spawn(move || {
            let mut live_market_data = LiveMarketData::default();
            let mut idle_us: u64 = 0;
            loop {
                let mut did_work = false;
                trade_sub.do_work();
                depth_sub.do_work();

                while let Some(trade) = trade_sub.poll() {
                    did_work = true;
                    let price = trade.price;
                    let qty = trade.quantity;
                    let side = trade.side;
                    let sym_end = trade.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                    let symbol = std::str::from_utf8(&trade.symbol[..sym_end])
                        .unwrap_or("BTC_USDT");
                    let ts = unix_now_micros();

                    if state.market_fanout.subscriber_count() != 0 {
                        state
                            .market_fanout
                            .send_owned(ws_sbe::encode_trade(price, qty, side, ts, symbol));
                    }
                    let sbe_msgs = live_market_data.ingest(symbol, ts, price, qty);
                    state
                        .last_ticker
                        .insert(symbol.to_string(), Arc::from(sbe_msgs[0].as_slice()));
                    if state.market_fanout.subscriber_count() != 0 {
                        for sbe in sbe_msgs {
                            state.market_fanout.send_owned(sbe);
                        }
                    }
                }

                while let Some(depth_msg) = depth_sub.poll() {
                    did_work = true;
                    let DeskDepthMsg::Depth(evt) = depth_msg else {
                        continue;
                    };
                    let nb = evt.num_bids as usize;
                    let na = evt.num_asks as usize;
                    if nb == 0 || na == 0 {
                        continue;
                    }
                    let mut bids_raw = [(0.0f64, 0.0f64); 20];
                    let mut asks_raw = [(0.0f64, 0.0f64); 20];
                    bids_raw[..nb].copy_from_slice(&evt.bids[..nb]);
                    asks_raw[..na].copy_from_slice(&evt.asks[..na]);
                    let end = evt.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                    let symbol = std::str::from_utf8(&evt.symbol[..end]).unwrap_or("BTC_USDT");
                    let bids: Vec<(f64, f64)> = bids_raw[..nb]
                        .iter()
                        .filter(|(_, q)| *q > 0.0)
                        .map(|&(p, q)| (p, q))
                        .collect();
                    let asks: Vec<(f64, f64)> = asks_raw[..na]
                        .iter()
                        .filter(|(_, q)| *q > 0.0)
                        .map(|&(p, q)| (p, q))
                        .collect();
                    let sbe = ws_sbe::encode_depth(unix_now_micros(), symbol, &bids, &asks);
                    state
                        .last_depth
                        .insert(symbol.to_string(), Arc::from(sbe.as_slice()));
                    if state.market_fanout.subscriber_count() != 0 {
                        state.market_fanout.send_owned(sbe);
                    }
                }

                if !did_work {
                    idle_us = (idle_us * 2 + 10).min(500);
                    std::thread::sleep(std::time::Duration::from_micros(idle_us));
                } else {
                    idle_us = 0;
                }
            }
        })?;
    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .route(
            "/health",
            get(|| async { Json(json!({"status": "ok", "service": "market-data-gateway"})) }),
        )
        .with_state(state)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let read_pool = Arc::new(PublicReadActorPool::new());
    let market_fanout = Arc::new(MarketFanout::new_with_actors(read_pool.market_senders()));
    let state = AppState {
        market_fanout,
        last_depth: Arc::new(DashMap::new()),
        last_ticker: Arc::new(DashMap::new()),
        write_pool: Arc::new(WriteActorPool::new()),
        read_pool,
    };

    spawn_aeron_public_loop(state.clone())?;

    let port = std::env::var("MARKET_DATA_PORT").unwrap_or_else(|_| "4010".to_string());
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    let app = router(state).layer(cors);
    let addr = format!("0.0.0.0:{port}");
    tracing::info!("Market Data Gateway listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{is_public_channel, market_channel, PublicClientMsg};
    use lightning_exchange::ws_sbe;

    #[test]
    fn parse_subscribe_message() {
        let msg = PublicClientMsg::parse(
            r#"{"type":"subscribe","channels":["depth.BTC_USDT","ticker.BTC_USDT"]}"#,
        )
        .unwrap();
        match msg {
            PublicClientMsg::Subscribe { channels } => {
                assert_eq!(channels, vec!["depth.BTC_USDT", "ticker.BTC_USDT"]);
            }
            _ => panic!("expected subscribe"),
        }
    }

    #[test]
    fn public_channel_filter_rejects_private_names() {
        assert!(is_public_channel("depth.BTC_USDT"));
        assert!(is_public_channel("trades.BTC_USDT"));
        assert!(is_public_channel("agg_trades.BTC_USDT"));
        assert!(!is_public_channel("orders"));
        assert!(!is_public_channel("balance"));
    }

    #[test]
    fn market_channel_maps_sbe_frames() {
        assert_eq!(
            market_channel(&ws_sbe::encode_trade(1.0, 2.0, 0, 123, "BTC_USDT")),
            Some("trades.BTC_USDT".to_string())
        );
        assert_eq!(
            market_channel(&ws_sbe::encode_depth(
                123,
                "ETH_USDT",
                &[(1.0, 2.0)],
                &[(3.0, 4.0)]
            )),
            Some("depth.ETH_USDT".to_string())
        );
    }
}
