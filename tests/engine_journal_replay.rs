//! End-to-end engine-input journal test against a REAL ArchivingMediaDriver.
//!
//! Proves the recovery property the engine wiring relies on: a matching
//! engine rebuilt by replaying the recorded input stream reaches a book
//! state IDENTICAL to the engine that processed the stream live (P2b made
//! the engine deterministic precisely so this holds).
//!
//!   1. record the orders stream; publish a 300-op mixed workload
//!      (limit orders both sides, IOC/FOK/PostOnly, cancels);
//!   2. engine A consumes the LIVE stream and matches as it goes;
//!   3. "crash": engine A's book snapshot is taken, everything dropped;
//!   4. engine B starts empty, replays the recording through the same
//!      handler logic, then its book must equal A's snapshot exactly.

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aeron_wrapper::AeronClient;
use lightning_exchange::engine::{MatchingEngine, PoolConfig};
use lightning_exchange::order::{Order, Side, TimeInForce};
use lightning_exchange::sbe::{CancelOrderRequest, NewOrderRequest};
use lightning_exchange::transport::aeron_transport::{AeronOrderSubscriber, DeskOrderPublisher};
use lightning_exchange::transport::{InboundMsg, OrderSubscriber};
use lightning_exchange::transport::journal::{
    JournalRecorder, JournalReplayer, ORDERS_REPLAY_STREAM_OFFSET,
};

const CONTROL_PORT: u16 = 18930;
const ORDERS_STREAM: i32 = 977; // test-private stream id
const OPS: u64 = 300;

fn jar_path() -> Option<String> {
    [
        std::env::var("AERON_ALL_JAR").ok(),
        Some(
            "/home/alpha/work/3party/aeron/aeron-all/build/libs/aeron-all-1.52.0-SNAPSHOT.jar"
                .into(),
        ),
    ]
    .into_iter()
    .flatten()
    .find(|p| std::path::Path::new(p).exists())
}

struct DriverGuard {
    child: Child,
    dir: std::path::PathBuf,
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn start_archiving_driver(jar: &str) -> std::io::Result<(DriverGuard, String)> {
    let base = std::env::temp_dir().join(format!(
        "engine-journal-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&base)?;
    let aeron_dir = base.join("driver").to_string_lossy().into_owned();
    let archive_dir = base.join("archive").to_string_lossy().into_owned();
    let child = Command::new("java")
        .arg("--add-opens")
        .arg("java.base/jdk.internal.misc=ALL-UNNAMED")
        .arg("--add-opens")
        .arg("java.base/sun.nio.ch=ALL-UNNAMED")
        .arg("-cp")
        .arg(jar)
        .arg(format!("-Daeron.dir={aeron_dir}"))
        .arg(format!("-Daeron.archive.dir={archive_dir}"))
        .arg(format!(
            "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:{CONTROL_PORT}"
        ))
        .arg("-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0")
        .arg("-Daeron.archive.recording.events.enabled=false")
        .arg("-Daeron.archive.file.sync.level=2")
        .arg("-Daeron.archive.catalog.file.sync.level=2")
        .arg("-Daeron.threading.mode=SHARED")
        .arg("-Daeron.archive.threading.mode=SHARED")
        .arg("-Daeron.shared.idle.strategy=sleep-ns")
        .arg("io.aeron.archive.ArchivingMediaDriver")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok((DriverGuard { child, dir: base }, aeron_dir))
}

fn wait_for<T>(what: &str, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = probe() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

/// The exact inbound-message application the engine loop performs
/// (place/cancel + integer order construction). Shared by the live and the
/// replay consumers in this test so divergence can only come from
/// transport/recording — which is what we are testing.
fn apply(engine: &mut MatchingEngine, msg: &InboundMsg) {
    match msg {
        InboundMsg::NewOrder(req) => {
            let side = if req.side == 0 { Side::Buy } else { Side::Sell };
            let tif = match req.time_in_force {
                0 => TimeInForce::GTC,
                1 => TimeInForce::IOC,
                2 => TimeInForce::FOK,
                3 => TimeInForce::PostOnly,
                _ => TimeInForce::GTC,
            };
            let price_ticks: i64 = req.price_ticks;
            let quantity_lots: i64 = req.quantity_lots;
            let client_order_id: u64 = req.client_order_id;
            if quantity_lots <= 0 {
                return;
            }
            let order = if price_ticks == 0 {
                Order::new_market(client_order_id, side, quantity_lots, 0)
            } else {
                Order::new(client_order_id, side, price_ticks, quantity_lots, tif, 0)
            };
            let _ = engine.place_order(order);
        }
        InboundMsg::CancelOrder(req) => {
            let order_id: u64 = req.order_id;
            let _ = engine.cancel_order(order_id);
        }
    }
}

fn drain(
    client: &Arc<AeronClient>,
    sub: &mut AeronOrderSubscriber,
    engine: &mut MatchingEngine,
    quiet: Duration,
) -> u64 {
    let mut applied = 0u64;
    let mut last = Instant::now();
    loop {
        client.do_work();
        sub.do_work();
        let mut got = false;
        while let Some(msg) = sub.poll() {
            got = true;
            applied += 1;
            apply(engine, &msg);
        }
        if got {
            last = Instant::now();
        } else if last.elapsed() > quiet {
            return applied;
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

#[test]
fn replayed_engine_rebuilds_identical_book() {
    let Some(jar) = jar_path() else {
        eprintln!("skip: aeron-all jar not found");
        return;
    };
    let Ok((_driver, aeron_dir)) = start_archiving_driver(&jar) else {
        eprintln!("skip: cannot start ArchivingMediaDriver");
        return;
    };
    let client = wait_for("media driver", Duration::from_secs(20), || {
        AeronClient::new(&aeron_dir).ok().map(Arc::new)
    });
    let config = aeron_wrapper::ArchiveConfig {
        control_request_channel: format!("aeron:udp?endpoint=localhost:{CONTROL_PORT}"),
        ..aeron_wrapper::ArchiveConfig::default()
    };

    // Live consumer FIRST (engine A's input), then the recording, then flow.
    let mut live_sub = AeronOrderSubscriber::new(client.clone(), "aeron:ipc", ORDERS_STREAM)
        .expect("live subscriber");
    let _recorder = wait_for("archive connect", Duration::from_secs(20), || {
        JournalRecorder::start(&client, &config, "aeron:ipc", ORDERS_STREAM).ok()
    });
    let mut publisher =
        DeskOrderPublisher::new(client.clone(), "aeron:ipc", ORDERS_STREAM).expect("publisher");
    std::thread::sleep(Duration::from_millis(500)); // recording sub joins

    // ── publish a deterministic mixed workload ───────────────────────────
    let mut rng = Lcg(0xE16E_1234_5678_9ABC);
    let mut placed: Vec<u64> = Vec::new();
    for i in 1..=OPS {
        if i % 11 == 0 && !placed.is_empty() {
            let victim = placed[(rng.next() as usize) % placed.len()];
            let req = CancelOrderRequest {
                order_id: victim,
                participant_id: 1,
                response_stream_id: 200,
                _pad: [0; 4],
            };
            wait_for("publish cancel", Duration::from_secs(5), || {
                client.do_work();
                publisher.publish_cancel(&req).ok()
            });
        } else {
            let req = NewOrderRequest {
                client_order_id: i,
                participant_id: 1 + (rng.next() % 5),
                price_ticks: 90 + (rng.next() % 21) as i64,
                quantity_lots: 1 + (rng.next() % 50) as i64,
                side: (rng.next() % 2) as u8,
                time_in_force: (rng.next() % 4) as u8,
                response_stream_id: 200,
                reduce_only: 0,
                _pad: [0; 9],
                symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
            };
            placed.push(i);
            wait_for("publish order", Duration::from_secs(5), || {
                client.do_work();
                publisher.publish_new_order(&req).ok()
            });
        }
    }

    // ── engine A: live consumption ───────────────────────────────────────
    let mut engine_a = MatchingEngine::new(PoolConfig::default()).unwrap();
    let live_applied = drain(&client, &mut live_sub, &mut engine_a, Duration::from_secs(2));
    assert_eq!(live_applied, OPS, "engine A must see the whole workload");
    let bids_a = engine_a.get_top_levels(100, true);
    let asks_a = engine_a.get_top_levels(100, false);
    assert!(
        !bids_a.is_empty() || !asks_a.is_empty(),
        "workload must leave a non-trivial book"
    );
    drop(engine_a); // ← the "crash"

    // ── engine B: rebuild from the journal ───────────────────────────────
    let mut replayer = JournalReplayer::connect(&client, &config).expect("replayer");
    let recordings = replayer
        .recordings("aeron", ORDERS_STREAM)
        .expect("list recordings");
    assert!(!recordings.is_empty(), "input recording must exist");

    let replay_stream = ORDERS_STREAM + ORDERS_REPLAY_STREAM_OFFSET;
    let mut engine_b = MatchingEngine::new(PoolConfig::default()).unwrap();
    let mut replay_applied = 0u64;
    for rec in &recordings {
        let Some(_r) = replayer
            .replay_bounded(rec, "aeron:ipc", replay_stream)
            .expect("start replay")
        else {
            continue;
        };
        let mut replay_sub = AeronOrderSubscriber::new(client.clone(), "aeron:ipc", replay_stream)
            .expect("replay subscriber");
        // Bounded replay: image-close + drained ring = exact completion.
        loop {
            client.do_work();
            replay_sub.do_work();
            let mut got = false;
            while let Some(msg) = replay_sub.poll() {
                got = true;
                replay_applied += 1;
                apply(&mut engine_b, &msg);
            }
            if !got {
                if replay_sub.replay_image_closed() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
    assert_eq!(replay_applied, OPS, "journal must re-deliver every input op");

    // ── the property: bit-identical book state ───────────────────────────
    assert_eq!(
        engine_b.get_top_levels(100, true),
        bids_a,
        "replayed bid book must equal the live engine's"
    );
    assert_eq!(
        engine_b.get_top_levels(100, false),
        asks_a,
        "replayed ask book must equal the live engine's"
    );
}
