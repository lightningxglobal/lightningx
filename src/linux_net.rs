/// Linux-specific network optimisations.
///
/// Two independent mechanisms are provided:
///
/// 1. `SO_BUSY_POLL` — kernel spins on the NIC ring buffer for up to N
///    microseconds before blocking on epoll. Eliminates the interrupt-wakeup
///    latency that dominates sub-200µs per-frame RTT.  Typical gain: 10-40µs.
///    Set `BUSY_POLL_US=0` to disable.
///
/// 2. io_uring accept loop — a dedicated thread runs a single io_uring
///    instance just for `accept`. Each accepted fd is converted to a
///    `tokio::net::TcpStream` and sent to the multi-threaded tokio runtime
///    via an mpsc channel. This removes accept syscall overhead from the main
///    event loop and demonstrates the io_uring integration pattern.
///    NOTE: the WS read/write hot path still uses tokio epoll because
///    fastwebsockets/hyper require `AsyncRead`/`AsyncWrite`. Full io_uring
///    for reads/writes would require replacing those with io_uring-native
///    equivalents (e.g. monoio).

use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// Apply `SO_BUSY_POLL` and `TCP_NODELAY` to a raw socket fd.
///
/// `busy_poll_us`: microseconds the kernel spins before sleeping.
/// 50µs is a good default for a co-located server.  Pass 0 to skip.
pub fn tune_accepted_socket(fd: RawFd, busy_poll_us: u32) {
    unsafe {
        // TCP_NODELAY — disable Nagle, send small frames immediately.
        let one: libc::c_int = 1;
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );

        if busy_poll_us > 0 {
            // SO_BUSY_POLL — spin on NIC ring for up to busy_poll_us µs.
            // Requires CAP_NET_ADMIN or a kernel with unprivileged busy-poll
            // enabled (net.core.busy_poll sysctl).
            let us = busy_poll_us as libc::c_int;
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                &us as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

fn busy_poll_us() -> u32 {
    std::env::var("BUSY_POLL_US")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
}

/// Run an io_uring accept loop on `addr` in a dedicated OS thread.
///
/// Each accepted connection is tuned (SO_BUSY_POLL + TCP_NODELAY) and then
/// sent as a `tokio::net::TcpStream` to the caller via `conn_tx`.
///
/// Call this BEFORE entering the tokio runtime (or from a `std::thread::spawn`
/// outside tokio) so the io_uring instance has its own event loop.
///
/// Returns immediately; the loop runs until `conn_tx` is dropped.
pub fn spawn_io_uring_accept_loop(
    addr: SocketAddr,
    conn_tx: mpsc::Sender<(TcpStream, SocketAddr)>,
) {
    let bpu = busy_poll_us();
    std::thread::Builder::new()
        .name("io-uring-accept".into())
        .spawn(move || {
            tokio_uring::start(async move {
                let listener = match tokio_uring::net::TcpListener::bind(addr) {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!("io_uring bind {addr}: {e}");
                        return;
                    }
                };
                tracing::info!("io_uring accept loop on {addr}");
                loop {
                    let (uring_stream, peer) = match listener.accept().await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("io_uring accept: {e}");
                            continue;
                        }
                    };

                    // Tune the socket before handing it off.
                    let fd = uring_stream.as_raw_fd();
                    tune_accepted_socket(fd, bpu);

                    // Transfer fd ownership from tokio-uring to tokio.
                    // `std::mem::forget` prevents tokio-uring from closing
                    // the fd; tokio takes ownership via from_raw_fd.
                    std::mem::forget(uring_stream);
                    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
                    if std_stream.set_nonblocking(true).is_err() {
                        continue;
                    }
                    let tokio_stream = match TcpStream::from_std(std_stream) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("TcpStream::from_std: {e}");
                            continue;
                        }
                    };

                    if conn_tx.send((tokio_stream, peer)).await.is_err() {
                        // Receiver dropped — main loop is shutting down.
                        break;
                    }
                }
            });
        })
        .expect("failed to spawn io-uring-accept thread");
}
