/// Read actor pool: N tasks each driving M connections via FuturesUnordered.
///
/// Market data distribution is lifted to the actor level: instead of each
/// connection's future waiting on a per-connection `market_rx` channel (100K
/// wakeups per broadcast), each actor receives one message from a shared
/// `market_rx` and fans it out to its slice of connections sequentially.
///
/// Scheduling benefit:
///   Before: 100K market broadcasts → 100K FuturesUnordered wakeups
///   After:  100K market broadcasts → 8 actor wakeups + 8 × (100K/8) sequential try_send
use crate::desk::write_actor::WsCtrl;
use dashmap::DashMap;
use fastwebsockets::WebSocketRead;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::io::ReadHalf;
use tokio::sync::mpsc;

pub type WsRead = WebSocketRead<ReadHalf<TokioIo<Upgraded>>>;

/// Per-connection market data state held in the actor's local registry.
pub struct ConnMarketInfo {
    /// Forward path to the write actor for this connection.
    pub personal_tx: mpsc::Sender<Vec<u8>>,
    /// Flipped to true when the client sends a Subscribe message.
    pub wants_market: Arc<AtomicBool>,
}

pub struct ReadConn {
    pub conn_id: u64,
    pub ws_read: WsRead,
    pub state: crate::desk::api::AppState,
    pub personal_tx: mpsc::Sender<Vec<u8>>,
    pub ctrl_tx: mpsc::Sender<WsCtrl>,
    /// Shared flag — ws_handler sets true on subscribe; actor checks before sending.
    pub wants_market: Arc<AtomicBool>,
}

fn read_actor_count() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("READ_ACTORS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8)
    })
}

async fn read_actor_loop(
    mut conn_rx: mpsc::Receiver<ReadConn>,
    mut market_rx: mpsc::Receiver<Arc<[u8]>>,
) {
    let actor_conns: Arc<DashMap<u64, ConnMarketInfo>> = Arc::new(DashMap::new());
    // How many connections in this actor are subscribed to market data.
    // Checked before DashMap iteration — stays O(1) when nobody subscribes
    // (e.g. trading-only bots or load-test connections).
    let actor_sub_count = Arc::new(AtomicUsize::new(0));
    let mut pending: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();

    loop {
        tokio::select! {
            // New connection arrives from ws_handler.
            msg = conn_rx.recv() => {
                match msg {
                    Some(c) => {
                        actor_conns.insert(c.conn_id, ConnMarketInfo {
                            personal_tx: c.personal_tx.clone(),
                            wants_market: c.wants_market.clone(),
                        });
                        let conns = actor_conns.clone();
                        let sub_cnt = actor_sub_count.clone();
                        pending.push(Box::pin(
                            crate::desk::ws_handler::read_conn_loop(c, conns, sub_cnt)
                        ));
                    }
                    None => break,
                }
            }

            // Connection reads — non-biased so tokio fairly interleaves with
            // the market data arm. biased; caused the actor to spin without
            // yielding the tokio worker, starving write actors.
            Some(_) = pending.next(), if !pending.is_empty() => {}

            // Market data from MarketFanout. actor_sub_count is 0 for connections
            // that never subscribed (trading bots, pressure tests) so this arm
            // is O(1) — one atomic load + branch — in that common case.
            Some(msg) = market_rx.recv() => {
                if actor_sub_count.load(Ordering::Relaxed) > 0 {
                    for entry in actor_conns.iter() {
                        let info = entry.value();
                        if info.wants_market.load(Ordering::Relaxed) {
                            let _ = info.personal_tx.try_send(msg.as_ref().to_owned());
                        }
                    }
                }
            }
        }
    }
    while pending.next().await.is_some() {}
}

pub struct ReadActorPool {
    senders: Vec<mpsc::Sender<ReadConn>>,
    next: AtomicUsize,
    /// Market senders — one per actor. Handed to MarketFanout at startup.
    market_txs: Vec<mpsc::Sender<Arc<[u8]>>>,
}

impl ReadActorPool {
    pub fn new() -> Self {
        let n = read_actor_count();
        let mut senders = Vec::with_capacity(n);
        let mut market_txs = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel::<ReadConn>(1024);
            let (mtx, mrx) = mpsc::channel::<Arc<[u8]>>(128);
            tokio::spawn(read_actor_loop(rx, mrx));
            senders.push(tx);
            market_txs.push(mtx);
        }
        Self {
            senders,
            next: AtomicUsize::new(0),
            market_txs,
        }
    }

    /// Returns the market data senders to hand to `MarketFanout::new_with_actors`.
    pub fn market_senders(&self) -> Vec<mpsc::Sender<Arc<[u8]>>> {
        self.market_txs.clone()
    }

    pub fn register(&self, conn: ReadConn) {
        let n = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        let _ = self.senders[n].try_send(conn);
    }
}

impl Default for ReadActorPool {
    fn default() -> Self {
        Self::new()
    }
}
