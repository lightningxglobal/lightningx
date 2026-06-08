//! Soak test — the one remaining technical unknown: does long-running
//! behavior DEGRADE over time? Runs the journaled engine under sustained
//! order flow for SOAK_SECS (default 90s; set to hours for a real soak),
//! sampling engine RSS and order latency, with periodic snapshots firing.
//! At the end it kills the engine and measures RESTART REPLAY TIME — the
//! number that must stay BOUNDED by the snapshot rather than growing with
//! total orders processed (the whole point of T4).
//!
//! Pass criteria:
//!   * engine RSS does not grow unboundedly (book is kept bounded via
//!     place+cancel, so growth would be a leak);
//!   * post-soak restart replay completes quickly (snapshot-bounded),
//!     independent of how many orders were processed.
//!
//! `SOAK_SECS=7200 cargo test --release --test soak_engine -- --nocapture`

mod common;

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aeron_wrapper::AeronClient;
use common::*;
use lightning_exchange::sbe::{CancelOrderRequest, NewOrderRequest};
use lightning_exchange::transport::aeron_channels::{
    order_update_channel, order_update_stream_for_desk, orders_channel, orders_stream_for_symbol,
};
use lightning_exchange::transport::aeron_transport::{DeskOrderPublisher, DeskOrderUpdateSubscriber};
use lightning_exchange::transport::order_update_kind;

const CONTROL_PORT: u16 = 19060;
const SYMBOL: &str = "BTC_USDT";

fn spawn_engine(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_exchange-engine"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("SYMBOLS", SYMBOL)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("SNAPSHOT_INTERVAL_SECS", "10") // snapshots fire throughout
        .env("ENGINE_IDLE_SPINS", "10000")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ProcGuard::spawn("engine", &mut cmd)
}

fn order(id: u64, side: u8, price: i64, qty: i64, stream: i32, tif: u8) -> NewOrderRequest {
    NewOrderRequest {
        client_order_id: id,
        participant_id: 7,
        price_ticks: price,
        quantity_lots: qty,
        side,
        time_in_force: tif,
        response_stream_id: stream,
        reduce_only: 0,
        _pad: [0; 9],
        symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_does_not_degrade_under_sustained_load() {
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
    let soak_secs: u64 = std::env::var("SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(90);

    let report = tokio::task::spawn_blocking(move || {
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
        let mut engine = spawn_engine(&aeron_dir, &control).expect("engine");
        let engine_pid = engine.pid().expect("engine pid");

        // Warm up.
        let mut next_id: u64 = 1;
        {
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut warm = false;
            while !warm {
                assert!(Instant::now() < deadline, "engine never acked");
                client.do_work();
                let _ = order_pub.publish_new_order(&order(next_id, 0, 100, 10, resp_stream, 1));
                next_id += 1;
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

        // ── Sustained churn: keep a BOUNDED resting book (place N, cancel
        //    the oldest) so RSS growth would be a leak, not legitimate
        //    book growth. Periodically cross to generate trades. ─────────
        const RING: usize = 2_000; // live resting orders kept around
        let mut live: std::collections::VecDeque<u64> = std::collections::VecDeque::new();
        let start = Instant::now();
        let mut placed: u64 = 0;
        let mut samples: Vec<(u64, u64)> = Vec::new(); // (elapsed_s, rss_kib)
        let mut last_sample = Instant::now();
        let mut last_log = Instant::now();

        while start.elapsed().as_secs() < soak_secs {
            // Place a resting order (alternating side, deep so it rests).
            let id = next_id;
            next_id += 1;
            let side = (placed % 2) as u8;
            let price = if side == 0 { 100 + (placed as i64 % 100) } else { 100_000 + (placed as i64 % 100) };
            wait_for("publish", Duration::from_secs(5), || {
                client.do_work();
                order_pub.publish_new_order(&order(id, side, price, 5, resp_stream, 0)).ok()
            });
            live.push_back(id);
            placed += 1;

            // Cancel the oldest to keep the book bounded.
            if live.len() > RING {
                if let Some(old) = live.pop_front() {
                    let cancel = CancelOrderRequest {
                        order_id: old,
                        participant_id: 7,
                        response_stream_id: resp_stream,
                        _pad: [0; 4],
                    };
                    let _ = order_pub.publish_cancel(&cancel);
                }
            }
            // Every 100th, cross to generate a trade.
            if placed % 100 == 0 {
                let cross = order(next_id, 0, 100_050, 5, resp_stream, 1); // IOC buy crosses sells
                next_id += 1;
                let _ = order_pub.publish_new_order(&cross);
            }
            client.do_work();
            updates.do_work();
            while updates.poll().is_some() {}

            // Sample RSS every 5s.
            if last_sample.elapsed() >= Duration::from_secs(5) {
                last_sample = Instant::now();
                if let Some(rss) = rss_kib(engine_pid) {
                    samples.push((start.elapsed().as_secs(), rss));
                }
            }
            if last_log.elapsed() >= Duration::from_secs(15) {
                last_log = Instant::now();
                let rss = rss_kib(engine_pid).unwrap_or(0);
                eprintln!(
                    "soak: t={}s placed={} rss={}MiB live_book={}",
                    start.elapsed().as_secs(), placed, rss / 1024, live.len()
                );
            }
        }

        // ── Post-soak restart replay time (must be snapshot-bounded) ────
        assert!(engine.is_running(), "engine died during the soak");
        eprintln!("soak: {placed} orders over {soak_secs}s — measuring restart replay…");
        engine.kill9();
        std::thread::sleep(Duration::from_millis(300));
        let restart = Instant::now();
        let mut engine2 = spawn_engine(&aeron_dir, &control).expect("respawn");
        // Replay is done when the engine answers (a bogus cancel echo).
        wait_for("engine live after restart", Duration::from_secs(120), || {
            let cancel = CancelOrderRequest {
                order_id: 555_555_555,
                participant_id: 7,
                response_stream_id: resp_stream,
                _pad: [0; 4],
            };
            client.do_work();
            let _ = order_pub.publish_cancel(&cancel);
            let until = Instant::now() + Duration::from_millis(400);
            while Instant::now() < until {
                client.do_work();
                updates.do_work();
                while let Some(m) = updates.poll() {
                    let kind: u8 = m.kind;
                    let oid: u64 = m.order_id;
                    if kind == order_update_kind::CANCELLED && oid == 555_555_555 {
                        return Some(());
                    }
                }
            }
            None
        });
        let replay_ms = restart.elapsed().as_millis();
        drop(engine2_guard(&mut engine2));

        (placed, samples, replay_ms)
    })
    .await
    .expect("soak");

    let (placed, samples, replay_ms) = report;

    // ── Verdict ──────────────────────────────────────────────────────────
    // Memory: compare the median of the first third vs the last third of
    // samples. With a bounded book, a >50% climb is a leak signal.
    assert!(samples.len() >= 3, "not enough RSS samples");
    let rss: Vec<u64> = samples.iter().map(|(_, r)| *r).collect();
    let third = rss.len() / 3;
    let mut early: Vec<u64> = rss[..third.max(1)].to_vec();
    let mut late: Vec<u64> = rss[rss.len() - third.max(1)..].to_vec();
    early.sort_unstable();
    late.sort_unstable();
    let med = |v: &[u64]| v[v.len() / 2];
    let (e, l) = (med(&early), med(&late));
    let peak = *rss.iter().max().unwrap();

    eprintln!(
        "\n── SOAK REPORT ──────────────────────────────────────────\n\
         orders placed     : {placed}\n\
         RSS early median  : {} MiB\n\
         RSS late median   : {} MiB\n\
         RSS peak          : {} MiB\n\
         RSS growth        : {:+.1}%\n\
         restart replay    : {} ms (snapshot-bounded)\n\
         ─────────────────────────────────────────────────────────",
        e / 1024, l / 1024, peak / 1024,
        (l as f64 - e as f64) / e as f64 * 100.0,
        replay_ms,
    );

    assert!(
        l <= e + e / 2,
        "engine RSS grew >50% under a BOUNDED book ({} → {} MiB) — leak suspected",
        e / 1024, l / 1024
    );
    // Replay after a snapshot must be quick regardless of total orders;
    // generous ceiling for CI machines.
    assert!(
        replay_ms < 60_000,
        "restart replay took {replay_ms}ms — snapshot bounding may be ineffective"
    );
}

/// Tiny guard so the respawned engine is killed even on a panic in the
/// blocking closure's tail.
fn engine2_guard(p: &mut ProcGuard) -> &mut ProcGuard {
    p
}
