/// Write actor pool: N tasks each driving M connections via FuturesUnordered.
///
/// Instead of one handler task per connection doing both reads and writes
/// (Option A doubled task count to 40K), we keep handler tasks read-only
/// and route all outgoing frames through a small fixed pool of write actors.
///
/// Scheduling benefit: at 20K connections, a burst of 20K order responses
/// wakes 8 write actors instead of 20K handler tasks — 2500× fewer scheduler
/// round-trips for the write path.
use fastwebsockets::{Frame, Payload, WebSocketWrite};
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use tokio::io::WriteHalf;
use tokio::sync::mpsc;

pub type WsWrite = WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>;

fn write_actor_count() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("WRITE_ACTORS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8)
    })
}

struct NewConn {
    ws_write: WsWrite,
    text_rx: mpsc::Receiver<String>,
    pong_rx: mpsc::Receiver<Vec<u8>>,
}

async fn write_conn_loop(
    mut ws: WsWrite,
    mut text_rx: mpsc::Receiver<String>,
    mut pong_rx: mpsc::Receiver<Vec<u8>>,
) {
    loop {
        tokio::select! {
            biased;
            // Pong responses are rare but must go out quickly for keep-alive.
            payload = pong_rx.recv() => {
                match payload {
                    Some(p) => {
                        if ws.write_frame(Frame::pong(Payload::Owned(p))).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            msg = text_rx.recv() => {
                match msg {
                    Some(s) => {
                        if ws.write_frame(Frame::text(Payload::Owned(s.into_bytes()))).await.is_err() {
                            break;
                        }
                    }
                    // personal_tx dropped → handler disconnected, stop writing.
                    None => break,
                }
            }
        }
    }
}

async fn write_actor_loop(mut conn_rx: mpsc::Receiver<NewConn>) {
    let mut pending: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
    loop {
        tokio::select! {
            msg = conn_rx.recv() => {
                match msg {
                    Some(c) => pending.push(Box::pin(write_conn_loop(
                        c.ws_write, c.text_rx, c.pong_rx,
                    ))),
                    None => break,
                }
            }
            // Only arm this branch when there are active connections.
            Some(_) = pending.next(), if !pending.is_empty() => {}
        }
    }
    while pending.next().await.is_some() {}
}

pub struct WriteActorPool {
    senders: Vec<mpsc::Sender<NewConn>>,
    next: AtomicUsize,
}

impl WriteActorPool {
    pub fn new() -> Self {
        let n = write_actor_count();
        let mut senders = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel::<NewConn>(1024);
            tokio::spawn(write_actor_loop(rx));
            senders.push(tx);
        }
        Self {
            senders,
            next: AtomicUsize::new(0),
        }
    }

    /// Register a new connection's write half. Returns `(text_tx, pong_tx)`.
    ///
    /// `text_tx` — store in `user_tx` registry; used by spin thread and
    ///   `handle_client_message` for all outgoing text frames.
    /// `pong_tx` — kept by the handler task; used to forward Pong responses
    ///   when the client sends a Ping frame.
    ///
    /// Returns `None` if the actor's registration channel is unexpectedly full
    /// (capacity 1024 — should never occur under normal connection rates).
    pub fn register(
        &self,
        ws_write: WsWrite,
        text_cap: usize,
    ) -> Option<(mpsc::Sender<String>, mpsc::Sender<Vec<u8>>)> {
        let (text_tx, text_rx) = mpsc::channel::<String>(text_cap);
        let (pong_tx, pong_rx) = mpsc::channel::<Vec<u8>>(4);
        let n = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        self.senders[n]
            .try_send(NewConn {
                ws_write,
                text_rx,
                pong_rx,
            })
            .ok()?;
        Some((text_tx, pong_tx))
    }
}

impl Default for WriteActorPool {
    fn default() -> Self {
        Self::new()
    }
}
