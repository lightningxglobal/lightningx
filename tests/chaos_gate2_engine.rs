//! Gate 2, engine clause — the REAL exchange-engine binary SIGKILLed
//! repeatedly under order flow; after the final restart its book must be
//! IDENTICAL to a reference engine that saw every order exactly once.
//!
//! The property under test: orders published while the engine is DEAD are
//! not lost — the archive's recording subscription outlives the engine, so
//! a restart's journal replay re-delivers them, and the deterministic
//! engine (P2b) reconverges to the reference state.
//!
//! Verified after N kills (default 5; CHAOS_KILLS_ENGINE=… to scale):
//!   1. sampled CANCELs of orders resting since before various kills
//!      succeed — order ids and book entries survived every crash;
//!   2. the top-20 depth snapshot equals the reference book exactly.

mod common;

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aeron_wrapper::AeronClient;
use common::*;
use lightning_exchange::db;
use lightning_exchange::engine::{MatchingEngine, PoolConfig};
use lightning_exchange::order::{Order, Side, TimeInForce};
use lightning_exchange::sbe::{CancelOrderRequest, NewOrderRequest};
use lightning_exchange::transport::aeron_channels::{
    DEPTH50_STREAM, DEPTH_STREAM, LEVEL2_STREAM, depth_channel, order_update_channel,
    order_update_stream_for_desk, orders_channel, orders_stream_for_symbol,
};
use lightning_exchange::transport::aeron_transport::{
    DeskDepthMsg, DeskDepthSubscriber, DeskOrderPublisher, DeskOrderUpdateSubscriber,
};
use lightning_exchange::transport::order_update_kind;

const CONTROL_PORT: u16 = 18970;
const SYMBOL: &str = "BTC_USDT";
const OPS: u64 = 600;

fn spawn_engine(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_exchange-engine"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("SYMBOLS", SYMBOL)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("ENGINE_IDLE_SPINS", "1000")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ProcGuard::spawn("engine", &mut cmd)
}

fn req_tif(id: u64, side: u8, price_ticks: i64, qty: i64, stream: i32, tif: u8) -> NewOrderRequest {
    NewOrderRequest {
        client_order_id: id,
        participant_id: 5,
        price_ticks,
        quantity_lots: qty,
        side,
        time_in_force: tif,
        response_stream_id: stream,
        _pad: [0; 10],
        symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
    }
}

fn req(id: u64, side: u8, price_ticks: i64, qty: i64, stream: i32) -> NewOrderRequest {
    req_tif(id, side, price_ticks, qty, stream, 0) // GTC
}

/// EXACTLY the engine's GTC handling for in-band orders (same as the
/// engine_journal_replay harness): apply to the reference book.
fn apply_ref(engine: &mut MatchingEngine, side: u8, id: u64, price: i64, qty: i64) {
    let side = if side == 0 { Side::Buy } else { Side::Sell };
    let _ = engine.place_order(Order::new(id, side, price, qty, TimeInForce::GTC, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_killed_repeatedly_reconverges_to_reference_book() {
    let Some(jar) = jar_path() else {
        eprintln!("skip: aeron-all jar not found");
        return;
    };
    let Some(pg) = db::create_pool_sized(&pg_url(), 2).await.ok() else {
        eprintln!("skip: no PG");
        return;
    };
    db::run_migrations(&pg).await.expect("migrations");
    let Ok(driver) = start_archiving_driver(&jar, CONTROL_PORT) else {
        eprintln!("skip: cannot start ArchivingMediaDriver");
        return;
    };
    let aeron_dir = driver.aeron_dir.clone();
    let control = driver.control_channel.clone();
    let _drill_guard = common::DrillGuard::acquire();
    let kills: u32 = std::env::var("CHAOS_KILLS_ENGINE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    tokio::task::spawn_blocking(move || {
        let client = wait_for("media driver", Duration::from_secs(20), || {
            AeronClient::new(&aeron_dir).ok().map(Arc::new)
        });
        let orders_stream = orders_stream_for_symbol(SYMBOL);
        let resp_stream = order_update_stream_for_desk(0);
        let mut order_pub =
            DeskOrderPublisher::new(client.clone(), &orders_channel(), orders_stream)
                .expect("order publisher");
        let mut updates =
            DeskOrderUpdateSubscriber::new(client.clone(), &order_update_channel(), resp_stream)
                .expect("update subscriber");
        let mut depth = DeskDepthSubscriber::new(
            client.clone(),
            &depth_channel(),
            DEPTH_STREAM,
            DEPTH50_STREAM,
            LEVEL2_STREAM,
        )
        .expect("depth subscriber");

        let mut reference = MatchingEngine::new(PoolConfig::default()).unwrap();
        let mut engine = spawn_engine(&aeron_dir, &control).expect("spawn engine");
        let mut next_id: u64 = 1;

        // ── Warm-up: prove the path before single-shot semantics ─────────
        // Probes are IOC: an IOC buy on an empty book fills nothing and
        // RESTS nothing, so probes leave zero book footprint whether the
        // engine saw them live, sees them only via journal replay, or
        // never saw them at all (published before its subscription
        // joined). The reference book ignores them entirely.
        {
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut warmed = false;
            while !warmed {
                assert!(Instant::now() < deadline, "engine never acked warm-up");
                let r = req_tif(next_id, 0, 100, 10, resp_stream, 1); // IOC probe
                client.do_work();
                if order_pub.publish_new_order(&r).is_ok() {
                    next_id += 1;
                }
                let until = Instant::now() + Duration::from_millis(400);
                while Instant::now() < until {
                    client.do_work();
                    updates.do_work();
                    while updates.poll().is_some() {
                        warmed = true;
                    }
                }
            }
        }
        eprintln!("engine-chaos: path warmed up");

        // ── Order flow with kills interleaved ────────────────────────────
        // Deterministic mixed GTC workload in 90..=110; some orders land
        // while the engine is DEAD (recorded only — must come back).
        let mut rng = Lcg(0xE16E_C4A0_5BAD_F00D);
        let mut resting_candidates: Vec<u64> = Vec::new();
        let kill_every = OPS / (kills as u64 + 1);
        let mut killed = 0u32;
        for i in 0..OPS {
            let side = (rng.next() % 2) as u8;
            // Buys low (90..100), sells high (101..110): most orders REST,
            // giving the final book real shape; crosses still happen at
            // the boundary regions via market drift.
            let price = if side == 0 {
                90 + (rng.range(0, 10)) as i64
            } else {
                101 + (rng.range(0, 9)) as i64
            };
            let qty = 1 + (rng.range(0, 30)) as i64;
            let r = req(next_id, side, price, qty, resp_stream);
            wait_for("publish", Duration::from_secs(5), || {
                client.do_work();
                order_pub.publish_new_order(&r).ok()
            });
            apply_ref(&mut reference, side, next_id, price, qty);
            if side == 1 && price >= 105 {
                resting_candidates.push(next_id); // deep asks: likely resting
            }
            next_id += 1;
            std::thread::sleep(Duration::from_millis(3));

            if killed < kills && i > 0 && i % kill_every == 0 {
                killed += 1;
                eprintln!("engine-chaos: SIGKILL #{killed}/{kills} at op {i}");
                engine.kill9();
                // Keep publishing while dead for a moment (recorded only).
                std::thread::sleep(Duration::from_millis(300));
                engine = spawn_engine(&aeron_dir, &control).expect("respawn engine");
            }
        }

        // ── Final convergence: wait until replay finished and live ───────
        // The engine answers cancels only when live; probe with a cancel
        // of a known-bogus id until the CANCELLED(0) echo arrives.
        wait_for("engine live after final restart", Duration::from_secs(60), || {
            let cancel = CancelOrderRequest {
                order_id: 888_888_888,
                participant_id: 5,
                response_stream_id: resp_stream,
                _pad: [0; 4],
            };
            client.do_work();
            let _ = order_pub.publish_cancel(&cancel);
            let until = Instant::now() + Duration::from_millis(400);
            while Instant::now() < until {
                client.do_work();
                updates.do_work();
                while let Some(msg) = updates.poll() {
                    let kind: u8 = msg.kind;
                    let oid: u64 = msg.order_id;
                    if kind == order_update_kind::CANCELLED && oid == 888_888_888 {
                        return Some(());
                    }
                }
            }
            None
        });
        eprintln!("engine-chaos: engine live after {kills} kills");

        // ── Proof 1: cancel orders that survived MULTIPLE crashes ────────
        let mut proven = 0;
        for &oid in resting_candidates.iter().take(8) {
            if !reference.contains_order(oid) {
                continue; // got filled in the reference too — skip
            }
            let cancel = CancelOrderRequest {
                order_id: oid,
                participant_id: 5,
                response_stream_id: resp_stream,
                _pad: [0; 4],
            };
            wait_for("publish cancel", Duration::from_secs(5), || {
                client.do_work();
                order_pub.publish_cancel(&cancel).ok()
            });
            let qty = wait_for("cancel ack", Duration::from_secs(10), || {
                client.do_work();
                updates.do_work();
                while let Some(msg) = updates.poll() {
                    let kind: u8 = msg.kind;
                    let o: u64 = msg.order_id;
                    if kind == order_update_kind::CANCELLED && o == oid {
                        let q: f64 = msg.fill_qty;
                        return Some(q);
                    }
                }
                None
            });
            assert!(
                qty > 0.0,
                "order {oid} should still be resting with quantity (book survived)"
            );
            let _ = reference.cancel_order(oid);
            proven += 1;
        }
        assert!(proven >= 3, "need several surviving resting orders, got {proven}");
        eprintln!("engine-chaos: {proven} pre-crash resting orders cancelled successfully");

        // ── Proof 2: top-20 depth equals the reference book ──────────────
        // Drain stale snapshots, then take the next fresh one.
        let until = Instant::now() + Duration::from_millis(300);
        while Instant::now() < until {
            client.do_work();
            depth.do_work();
            while depth.poll().is_some() {}
        }
        let snap = wait_for("fresh depth snapshot", Duration::from_secs(10), || {
            client.do_work();
            depth.do_work();
            let mut latest = None;
            while let Some(msg) = depth.poll() {
                if let DeskDepthMsg::Depth(evt) = msg {
                    latest = Some(evt);
                }
            }
            latest
        });

        let ref_bids = reference.get_top_levels(20, true);
        let ref_asks = reference.get_top_levels(20, false);
        assert_eq!(snap.num_bids as usize, ref_bids.len().min(20), "bid depth");
        assert_eq!(snap.num_asks as usize, ref_asks.len().min(20), "ask depth");
        for (i, &(p_ticks, q_lots)) in ref_bids.iter().take(20).enumerate() {
            let (sp, sq) = snap.bids[i];
            assert!(
                (sp - p_ticks as f64 * 0.01).abs() < 1e-9
                    && (sq - q_lots as f64 * 1e-6).abs() < 1e-9,
                "bid level {i} diverged: engine=({sp},{sq}) ref=({p_ticks},{q_lots})"
            );
        }
        for (i, &(p_ticks, q_lots)) in ref_asks.iter().take(20).enumerate() {
            let (sp, sq) = snap.asks[i];
            assert!(
                (sp - p_ticks as f64 * 0.01).abs() < 1e-9
                    && (sq - q_lots as f64 * 1e-6).abs() < 1e-9,
                "ask level {i} diverged: engine=({sp},{sq}) ref=({p_ticks},{q_lots})"
            );
        }
        eprintln!(
            "GATE 2 (engine) PASS: {kills} SIGKILLs over {OPS} ops — book identical to reference"
        );
    })
    .await
    .expect("drill");
    let _ = pg;
}
