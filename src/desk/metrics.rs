//! Zero-dependency Prometheus text-format metrics.
//!
//! Scraped by the already-deployed VictoriaMetrics (drop-in compatible with
//! the Prometheus exposition format). Counters are process-global atomics —
//! one `fetch_add` per event, nothing on the matching path. Gauges are
//! sampled at render time through callbacks, so queue depths and cache
//! sizes cost nothing between scrapes.
//!
//! Usage: `metrics::counter("orders_rejected_total").inc()`;
//! `metrics::register_gauge("persist_queue_depth", || q.len() as f64)`;
//! expose `metrics::render()` on a /metrics endpoint.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

pub struct Counter(AtomicU64);

impl Counter {
    #[inline]
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

type GaugeFn = Box<dyn Fn() -> f64 + Send + Sync>;

#[derive(Default)]
struct Registry {
    counters: BTreeMap<String, Arc<Counter>>,
    gauges: BTreeMap<String, GaugeFn>,
}

fn registry() -> &'static Mutex<Registry> {
    static REG: OnceLock<Mutex<Registry>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(Registry::default()))
}

/// Get or create a process-global counter. Cache the Arc at the call site
/// when the increment is hot — the lookup takes the registry lock.
pub fn counter(name: &str) -> Arc<Counter> {
    let mut reg = registry().lock().unwrap();
    reg.counters
        .entry(name.to_string())
        .or_insert_with(|| Arc::new(Counter(AtomicU64::new(0))))
        .clone()
}

/// Register (or replace) a sampled gauge.
pub fn register_gauge(name: &str, f: impl Fn() -> f64 + Send + Sync + 'static) {
    registry()
        .lock()
        .unwrap()
        .gauges
        .insert(name.to_string(), Box::new(f));
}

/// Render the Prometheus exposition text.
pub fn render() -> String {
    let reg = registry().lock().unwrap();
    let mut out = String::with_capacity(1024);
    for (name, c) in &reg.counters {
        out.push_str(&format!("# TYPE {name} counter\n{name} {}\n", c.get()));
    }
    for (name, g) in &reg.gauges {
        let v = g();
        out.push_str(&format!("# TYPE {name} gauge\n{name} {v}\n"));
    }
    out
}

/// Spawn a minimal HTTP listener serving /metrics (for processes without an
/// axum router, e.g. pg-writer). Plain HTTP/1.0 response; one connection at
/// a time is plenty for a scraper.
pub fn spawn_metrics_listener(addr: String) {
    std::thread::Builder::new()
        .name("metrics-http".into())
        .spawn(move || {
            let listener = match std::net::TcpListener::bind(&addr) {
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!("metrics listener bind {addr} failed: {e}");
                    return;
                }
            };
            tracing::info!("metrics listening on {addr}");
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { continue };
                use std::io::{Read, Write};
                let mut buf = [0u8; 1024];
                let _ = s.read(&mut buf); // drain request; path is irrelevant
                let body = render();
                let _ = write!(
                    s,
                    "HTTP/1.0 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
            }
        })
        .ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_gauges_render_exposition_format() {
        let c = counter("test_events_total");
        c.inc();
        c.add(2);
        assert_eq!(counter("test_events_total").get(), 3, "same instance");

        register_gauge("test_depth", || 42.5);
        let text = render();
        assert!(text.contains("# TYPE test_events_total counter\ntest_events_total 3\n"));
        assert!(text.contains("# TYPE test_depth gauge\ntest_depth 42.5\n"));
    }

    #[test]
    fn metrics_http_serves_render() {
        let c = counter("test_http_total");
        c.inc();
        spawn_metrics_listener("127.0.0.1:39184".into());
        std::thread::sleep(std::time::Duration::from_millis(200));
        use std::io::{Read, Write};
        let mut s = std::net::TcpStream::connect("127.0.0.1:39184").expect("connect");
        s.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        assert!(resp.starts_with("HTTP/1.0 200 OK"));
        assert!(resp.contains("test_http_total"));
    }
}
