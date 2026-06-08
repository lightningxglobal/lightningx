//! S8.1 — load test WITH the journal on (file.sync.level=2, every frame
//! fsynced). Measures sustained order throughput and end-to-end
//! publish→ACK latency percentiles through the REAL engine + archive,
//! and prints a report. This is the gate that decides whether the
//! production fsync level is viable on the target disk (runbook §8.1):
//! if p99 here is unacceptable on NVMe, drop to sync level 1 + a
//! battery-backed disk and re-argue RPO.
//!
//! Not a hard SLA assertion (numbers are hardware-dependent) — it
//! asserts only that the journaled path SUSTAINS a sane floor and
//! reports the full distribution. Scale with LOAD_ORDERS.

mod common;

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aeron_wrapper::AeronClient;
use common::*;
use lightning_exchange::sbe::NewOrderRequest;
use lightning_exchange::transport::aeron_channels::{
    order_update_channel, order_update_stream_for_desk, orders_channel, orders_stream_for_symbol,
};
use lightning_exchange::transport::aeron_transport::{DeskOrderPublisher, DeskOrderUpdateSubscriber};

const CONTROL_PORT: u16 = 19030;
const SYMBOL: &str = "BTC_USDT";

fn spawn_engine(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_exchange-engine"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("SYMBOLS", SYMBOL)
        .env("EXCHANGE_ARCHIVE_CONTROL", control) // journal ON
        .env("ENGINE_IDLE_SPINS", "10000")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ProcGuard::spawn("engine", &mut cmd)
}

fn order(id: u64, side: u8, price_ticks: i64, stream: i32) -> NewOrderRequest {
    NewOrderRequest {
        client_order_id: id,
        participant_id: 7,
        price_ticks,
        quantity_lots: 10,
        side,
        time_in_force: 0,
        response_stream_id: stream,
        reduce_only: 0,
        _pad: [0; 9],
        symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
    }
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journaled_order_throughput_and_latency() {
    let Some(jar) = jar_path() else {
        eprintln!("skip: aeron-all jar not found");
        return;
    };
    let Ok(driver) = start_archiving_driver(&jar, CONTROL_PORT) else {
        eprintln!("skip: cannot start ArchivingMediaDriver");
        return;
    };
    let aeron_dir = driver.aeron_dir.clone();
    let control = driver.control_channel.clone();
    let _drill_guard = common::DrillGuard::acquire();

    let total: u64 = std::env::var("LOAD_ORDERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);

    tokio::task::spawn_blocking(move || {
        let client = wait_for("media driver", Duration::from_secs(20), || {
            AeronClient::new(&aeron_dir).ok().map(Arc::new)
        });
        let orders_stream = orders_stream_for_symbol(SYMBOL);
        let resp_stream = order_update_stream_for_desk(0);
        let mut order_pub =
            DeskOrderPublisher::new(client.clone(), &orders_channel(), orders_stream)
                .expect("publisher");
        let mut updates =
            DeskOrderUpdateSubscriber::new(client.clone(), &order_update_channel(), resp_stream)
                .expect("subscriber");
        let _engine = spawn_engine(&aeron_dir, &control).expect("engine");

        // Warm up until the path is live.
        let mut next_id: u64 = 1;
        {
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut warm = false;
            while !warm {
                assert!(Instant::now() < deadline, "engine never acked");
                let r = order(next_id, 0, 100, resp_stream);
                next_id += 1;
                client.do_work();
                let _ = order_pub.publish_new_order(&r);
                let until = Instant::now() + Duration::from_millis(300);
                while Instant::now() < until {
                    client.do_work();
                    updates.do_work();
                    while updates.poll().is_some() {
                        warm = true;
                    }
                }
            }
        }

        // Resting GTC orders far apart so they ACCEPT (no fills): each ACK
        // is one journaled+matched order. Track per-order send→ACK latency.
        let mut latencies: Vec<u64> = Vec::with_capacity(total as usize);
        let mut sent_at: std::collections::HashMap<u64, Instant> = std::collections::HashMap::new();
        let start = Instant::now();
        let mut sent: u64 = 0;
        let mut acked: u64 = 0;
        let base_id = next_id;

        while acked < total {
            // Keep up to 256 orders in flight to load the pipe.
            while sent < total && sent - acked < 256 {
                let id = base_id + sent;
                // Alternate sides at distinct deep prices → all rest.
                let side = (sent % 2) as u8;
                let price = if side == 0 { 100 + (sent as i64 % 50) } else { 100_000 + (sent as i64 % 50) };
                let r = order(id, side, price, resp_stream);
                client.do_work();
                if order_pub.publish_new_order(&r).is_ok() {
                    sent_at.insert(id, Instant::now());
                    sent += 1;
                } else {
                    break; // backpressure — drain ACKs
                }
            }
            client.do_work();
            updates.do_work();
            while let Some(msg) = updates.poll() {
                let coid: u64 = msg.client_order_id;
                if let Some(t) = sent_at.remove(&coid) {
                    latencies.push(t.elapsed().as_micros() as u64);
                    acked += 1;
                }
            }
            assert!(
                start.elapsed() < Duration::from_secs(120),
                "load test stalled at {acked}/{total} acked"
            );
        }
        let elapsed = start.elapsed();

        latencies.sort_unstable();
        let throughput = total as f64 / elapsed.as_secs_f64();
        eprintln!(
            "\n── S8.1 JOURNALED LOAD (file.sync.level=2) ──────────────\n\
             orders        : {total}\n\
             wall          : {:.2}s\n\
             throughput    : {:.0} orders/s\n\
             latency p50   : {} µs\n\
             latency p90   : {} µs\n\
             latency p99   : {} µs\n\
             latency p99.9 : {} µs\n\
             latency max   : {} µs\n\
             ─────────────────────────────────────────────────────────",
            elapsed.as_secs_f64(),
            throughput,
            pct(&latencies, 0.50),
            pct(&latencies, 0.90),
            pct(&latencies, 0.99),
            pct(&latencies, 0.999),
            latencies.last().copied().unwrap_or(0),
        );

        // Sane floor only (hardware-dependent): the journaled path must
        // sustain at least 500 orders/s, or the fsync level needs review.
        assert!(
            throughput >= 500.0,
            "journaled throughput {throughput:.0}/s below the 500/s floor — \
             review disk / fsync level per runbook §8.1"
        );
    })
    .await
    .expect("load");
}
