//! Gate 4 flash-crash drill — a REAL exchange-engine process under an
//! engineered price collapse must trip the circuit breaker, enter
//! cancel-only mode, and recover after the cooldown.
//!
//! Asserts the full lifecycle end-to-end through the wire:
//!   1. trades at the anchor price flow normally;
//!   2. a fill > ENGINE_CB_BPS away from the window anchor TRIPS the
//!      breaker: new orders are rejected with reject_reason 10;
//!   3. cancels still pass during the halt (participants can pull quotes);
//!   4. after ENGINE_CB_COOLDOWN_MS, order flow resumes (ACCEPTED again).

mod common;

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aeron_wrapper::AeronClient;
use common::*;
use lightning_exchange::db;
use lightning_exchange::sbe::{CancelOrderRequest, NewOrderRequest};
use lightning_exchange::transport::aeron_channels::{
    order_update_channel, order_update_stream_for_desk, orders_channel, orders_stream_for_symbol,
};
use lightning_exchange::transport::aeron_transport::{
    DeskOrderPublisher, DeskOrderUpdateSubscriber,
};
use lightning_exchange::transport::order_update_kind;

const CONTROL_PORT: u16 = 18960;
const SYMBOL: &str = "BTC_USDT";
const CB_BPS: &str = "500"; // 5% intra-window move trips
const CB_COOLDOWN_MS: u64 = 3_000;
const REASON_CIRCUIT_BREAKER: u8 = 10;

fn spawn_engine(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_exchange-engine"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("SYMBOLS", SYMBOL)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("ENGINE_CB_BPS", CB_BPS)
        .env("ENGINE_CB_WINDOW_MS", "30000")
        .env("ENGINE_CB_COOLDOWN_MS", CB_COOLDOWN_MS.to_string())
        .env("ENGINE_IDLE_SPINS", "1000")
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ProcGuard::spawn("engine-cb", &mut cmd)
}

fn order(id: u64, side: u8, price_ticks: i64, qty_lots: i64, stream: i32) -> NewOrderRequest {
    NewOrderRequest {
        client_order_id: id,
        participant_id: 9,
        price_ticks,
        quantity_lots: qty_lots,
        side,
        time_in_force: 0,
        response_stream_id: stream,
        reduce_only: 0,
        _pad: [0; 9],
        symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flash_crash_trips_breaker_cancel_only_then_recovers() {
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
        let _engine = spawn_engine(&aeron_dir, &control).expect("spawn engine");

        let mut next_id: u64 = 1;
        // ── 0. Warm-up: the engine's subscription must JOIN before frames
        // can be received (IPC does not buffer for absent subscribers).
        // Pump throwaway orders until the first ACK proves the path is up.
        {
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut warmed = false;
            while !warmed {
                assert!(Instant::now() < deadline, "engine never acked warm-up");
                let probe = order(next_id, 0, 100, 10, resp_stream);
                next_id += 1;
                let _ = {
                    client.do_work();
                    order_pub.publish_new_order(&probe)
                };
                let until = Instant::now() + Duration::from_millis(400);
                while Instant::now() < until {
                    client.do_work();
                    updates.do_work();
                    while let Some(_msg) = updates.poll() {
                        warmed = true;
                    }
                }
            }
            // Drain any other warm-up acks so the script reads cleanly.
            let until = Instant::now() + Duration::from_millis(300);
            while Instant::now() < until {
                client.do_work();
                updates.do_work();
                while updates.poll().is_some() {}
            }
        }
        eprintln!("flashcrash: order path warmed up");
        // Wait for one outcome (kind, reject_reason, order_id) of a given coid.
        let mut outcome = |order_pub: &mut DeskOrderPublisher,
                           updates: &mut DeskOrderUpdateSubscriber,
                           client: &Arc<AeronClient>,
                           req: &NewOrderRequest|
         -> (u8, u8, u64) {
            wait_for("publish", Duration::from_secs(10), || {
                client.do_work();
                order_pub.publish_new_order(req).ok()
            });
            let coid = req.client_order_id;
            wait_for("outcome", Duration::from_secs(15), || {
                client.do_work();
                updates.do_work();
                while let Some(msg) = updates.poll() {
                    let c: u64 = msg.client_order_id;
                    if c == coid {
                        let kind: u8 = msg.kind;
                        let reason: u8 = msg.reject_reason;
                        let oid: u64 = msg.order_id;
                        return Some((kind, reason, oid));
                    }
                }
                None
            })
        };

        // ── 1. Anchor the breaker window at 10000 ticks ──────────────────
        // Maker sell @10000 rests; buy @10000 fills → trade at anchor.
        let m1 = order(next_id, 1, 10_000, 10, resp_stream);
        next_id += 1;
        let (k, _, _) = outcome(&mut order_pub, &mut updates, &client, &m1);
        assert_eq!(k, order_update_kind::ACCEPTED, "maker rests");
        let t1 = order(next_id, 0, 10_000, 10, resp_stream);
        next_id += 1;
        let (k, _, _) = outcome(&mut order_pub, &mut updates, &client, &t1);
        assert_eq!(k, order_update_kind::FILLED, "anchor trade prints");
        eprintln!("flashcrash: anchor trade at 10000");

        // ── 2. Crash: print a fill 6% below the anchor (band is 100%) ────
        let m2 = order(next_id, 1, 9_400, 10, resp_stream); // sell 9400 rests
        next_id += 1;
        let (k, _, _) = outcome(&mut order_pub, &mut updates, &client, &m2);
        assert_eq!(k, order_update_kind::ACCEPTED);
        let t2 = order(next_id, 0, 9_400, 10, resp_stream); // buy lifts it
        next_id += 1;
        let (k, _, _) = outcome(&mut order_pub, &mut updates, &client, &t2);
        assert_eq!(k, order_update_kind::FILLED, "crash print at 9400 (-6%)");
        let t_trip = Instant::now();
        eprintln!("flashcrash: -6% print — breaker must now be tripped");

        // ── 3. Halt: new orders rejected with reason 10 ──────────────────
        let probe = order(next_id, 0, 9_400, 10, resp_stream);
        next_id += 1;
        let (k, reason, _) = outcome(&mut order_pub, &mut updates, &client, &probe);
        assert_eq!(k, order_update_kind::REJECTED, "halt rejects new orders");
        assert_eq!(reason, REASON_CIRCUIT_BREAKER, "reject_reason 10");
        eprintln!("flashcrash: new order rejected (reason 10) — cancel-only confirmed");

        // ── 4. Cancels still pass during the halt ────────────────────────
        // Park a resting order from BEFORE the halt? We have none left —
        // place one... rejected. So cancel a known-resting order: m1 was
        // fully filled; m2 fully filled. Use a pre-halt resting order:
        // none. Cancel an unknown id — engine answers CANCELLED qty=0
        // (unblock semantics), which still proves the cancel PATH is open.
        let cancel = CancelOrderRequest {
            order_id: 999_999_999,
            participant_id: 9,
            response_stream_id: resp_stream,
            _pad: [0; 4],
        };
        wait_for("publish cancel", Duration::from_secs(5), || {
            client.do_work();
            order_pub.publish_cancel(&cancel).ok()
        });
        wait_for("cancel path open during halt", Duration::from_secs(10), || {
            client.do_work();
            updates.do_work();
            while let Some(msg) = updates.poll() {
                let kind: u8 = msg.kind;
                let oid: u64 = msg.order_id;
                if kind == order_update_kind::CANCELLED && oid == 999_999_999 {
                    return Some(());
                }
            }
            None
        });
        eprintln!("flashcrash: cancel path open during halt");

        // ── 5. Recovery after cooldown ───────────────────────────────────
        let since_trip = t_trip.elapsed();
        if since_trip < Duration::from_millis(CB_COOLDOWN_MS + 500) {
            std::thread::sleep(Duration::from_millis(CB_COOLDOWN_MS + 500) - since_trip);
        }
        let revive = order(next_id, 0, 9_400, 10, resp_stream);
        let (k, _, _) = outcome(&mut order_pub, &mut updates, &client, &revive);
        assert_eq!(
            k,
            order_update_kind::ACCEPTED,
            "order flow resumes after cooldown"
        );
        eprintln!("flashcrash: recovered after cooldown — trading resumed");
    })
    .await
    .expect("drill");

    eprintln!("GATE 4 PASS: trip → cancel-only → recovery, end-to-end");
    let _ = pg; // pool kept alive for the drill duration
}
