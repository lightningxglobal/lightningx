/// Read actor pool: N tasks each driving M connections via FuturesUnordered.
///
/// Mirrors write_actor.rs for the read side. Instead of 10K handler tasks
/// sleeping in select! loops, we have N read actors each driving up to
/// 10K/N connection read loops concurrently from a single sleeping task.
///
/// Scheduling benefit: a market data broadcast that wakes 10K connections
/// wakes N read actor tasks (not 10K handler tasks) — the tokio scheduler
/// ready-queue shrinks from 10K entries to N per event.
use crate::desk::write_actor::WsCtrl;
use fastwebsockets::WebSocketRead;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use tokio::io::ReadHalf;
use tokio::sync::mpsc;

pub type WsRead = WebSocketRead<ReadHalf<TokioIo<Upgraded>>>;

pub struct ReadConn {
    pub conn_id: u64,
    pub ws_read: WsRead,
    pub state: crate::desk::api::AppState,
    pub personal_tx: mpsc::Sender<Vec<u8>>,
    pub ctrl_tx: mpsc::Sender<WsCtrl>,
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

async fn read_actor_loop(mut conn_rx: mpsc::Receiver<ReadConn>) {
    let mut pending: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
    loop {
        tokio::select! {
            msg = conn_rx.recv() => {
                match msg {
                    Some(c) => pending.push(Box::pin(
                        crate::desk::ws_handler::read_conn_loop(c)
                    )),
                    None => break,
                }
            }
            Some(_) = pending.next(), if !pending.is_empty() => {}
        }
    }
    while pending.next().await.is_some() {}
}

pub struct ReadActorPool {
    senders: Vec<mpsc::Sender<ReadConn>>,
    next: AtomicUsize,
}

impl ReadActorPool {
    pub fn new() -> Self {
        let n = read_actor_count();
        let mut senders = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel::<ReadConn>(1024);
            tokio::spawn(read_actor_loop(rx));
            senders.push(tx);
        }
        Self {
            senders,
            next: AtomicUsize::new(0),
        }
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
