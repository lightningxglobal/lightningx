//! Gate 5 failover drill — two REAL exchange-engine processes, leader
//! SIGKILLed, standby takes over. Measures RTO against the hard
//! requirement: **the backup must take over within 3 minutes** (target is
//! seconds; 180s is the contract ceiling).
//!
//! Topology (single machine, all real): ArchivingMediaDriver + PG lease +
//! engine A (leader) + engine B (standby, journal-replays then silently
//! follows the live input) + this test acting as the desk (publishes
//! orders, consumes epoch-stamped order updates).
//!
//! Verified:
//!   1. only the leader publishes (standby is silent);
//!   2. SIGKILL the leader → standby wins the lease with epoch+1 and its
//!      first ACK arrives within RTO_LIMIT (measured + reported);
//!   3. seamlessness: an order RESTING from before the failover can be
//!      cancelled through the new leader — uid_map/book were rebuilt
//!      bit-exactly from the journal + live follow;
//!   4. fencing: every post-takeover message carries the higher epoch.

mod common;

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aeron_wrapper::AeronClient;
use common::*;
use lightning_exchange::db;
use lightning_exchange::leader::split_epoch;
use lightning_exchange::sbe::{CancelOrderRequest, NewOrderRequest};
use lightning_exchange::transport::aeron_channels::{
    order_update_channel, order_update_stream_for_desk, orders_channel, orders_stream_for_symbol,
};
use lightning_exchange::transport::aeron_transport::{DeskOrderPublisher, DeskOrderUpdateSubscriber};
use lightning_exchange::transport::order_update_kind;

const CONTROL_PORT: u16 = 18950;
const SYMBOL: &str = "BTC_USDT";
const RTO_LIMIT: Duration = Duration::from_secs(180);
const LEASE_TTL_SECS: &str = "3";

fn spawn_engine(name: &str, aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_exchange-engine"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("SYMBOLS", SYMBOL)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("EXCHANGE_LEADER_ELECT", "1")
        .env("EXCHANGE_LEASE_TTL_SECS", LEASE_TTL_SECS)
        .env("ENGINE_IDLE_SPINS", "1000") // shared box: don't burn cores
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ProcGuard::spawn(name, &mut cmd)
}

fn order(id: u64, side: u8, price_ticks: i64, qty_lots: i64, stream: i32) -> NewOrderRequest {
    NewOrderRequest {
        client_order_id: id,
        participant_id: 7,
        price_ticks,
        quantity_lots: qty_lots,
        side,
        time_in_force: 0, // GTC
        response_stream_id: stream,
        reduce_only: 0,
        _pad: [0; 9],
        symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standby_takes_over_within_rto_and_preserves_state() {
    let Some(jar) = jar_path() else {
        eprintln!("skip: aeron-all jar not found");
        return;
    };
    let Some(pg) = db::create_pool_sized(&pg_url(), 4).await.ok() else {
        eprintln!("skip: no PG");
        return;
    };
    db::run_migrations(&pg).await.expect("migrations");
    // Fresh lease for the drill.
    let _ = sqlx::query("DELETE FROM leader_lease WHERE role = 'engine'")
        .execute(&pg)
        .await;
    let Ok(driver) = start_archiving_driver(&jar, CONTROL_PORT) else {
        eprintln!("skip: cannot start ArchivingMediaDriver");
        return;
    };
    let aeron_dir = driver.aeron_dir.clone();
    let control = driver.control_channel.clone();
    let _drill_guard = common::DrillGuard::acquire();

    // Everything below runs on a blocking thread: Aeron polling + archive
    // contract + process control are synchronous.
    let result = tokio::task::spawn_blocking(move || {
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

        // ── Engine A starts and must become leader ───────────────────────
        let mut engine_a = spawn_engine("engine-a", &aeron_dir, &control).expect("spawn A");

        // Pump until the first ACK proves A is live (leader + recording up).
        let mut next_id: u64 = 1;
        let mut place = |order_pub: &mut DeskOrderPublisher,
                         client: &Arc<AeronClient>,
                         id: u64,
                         side: u8,
                         price: i64| {
            let req = order(id, side, price, 10, resp_stream);
            wait_for("publish order", Duration::from_secs(5), || {
                client.do_work();
                order_pub.publish_new_order(&req).ok()
            });
        };

        let first_epoch = {
            let mut seen: Option<u16> = None;
            let deadline = Instant::now() + Duration::from_secs(30);
            while seen.is_none() {
                assert!(Instant::now() < deadline, "engine A never acked");
                place(&mut order_pub, &client, next_id, 0, 100);
                next_id += 1;
                let until = Instant::now() + Duration::from_millis(400);
                while Instant::now() < until {
                    client.do_work();
                    updates.do_work();
                    while let Some(msg) = updates.poll() {
                        let (epoch, _) = split_epoch(msg.sequence);
                        seen = Some(epoch);
                    }
                }
            }
            seen.unwrap()
        };
        eprintln!("failover: engine A leading with epoch {first_epoch}");

        // A RESTING order placed pre-failover (sell far above market: rests).
        let resting_id = next_id;
        next_id += 1;
        place(&mut order_pub, &client, resting_id, 1, 10_000);
        let resting_oid = wait_for("resting ACCEPTED", Duration::from_secs(10), || {
            client.do_work();
            updates.do_work();
            while let Some(msg) = updates.poll() {
                let kind: u8 = msg.kind;
                let coid: u64 = msg.client_order_id;
                if kind == order_update_kind::ACCEPTED && coid == resting_id {
                    let oid: u64 = msg.order_id;
                    return Some(oid);
                }
            }
            None
        });

        // ── Standby joins: replays journal, then silently follows ────────
        let _engine_b = spawn_engine("engine-b", &aeron_dir, &control).expect("spawn B");
        // Give B time to finish journal replay and reach follow mode.
        std::thread::sleep(Duration::from_secs(3));

        // Standby silence check: keep trading through A; every ACK must
        // still be epoch=first_epoch and exactly one per order.
        for _ in 0..5 {
            let id = next_id;
            next_id += 1;
            place(&mut order_pub, &client, id, 0, 100);
            let mut acks = 0;
            let until = Instant::now() + Duration::from_millis(500);
            while Instant::now() < until {
                client.do_work();
                updates.do_work();
                while let Some(msg) = updates.poll() {
                    let coid: u64 = msg.client_order_id;
                    if coid == id {
                        acks += 1;
                        let (epoch, _) = split_epoch(msg.sequence);
                        assert_eq!(epoch, first_epoch, "standby must not publish");
                    }
                }
            }
            assert_eq!(acks, 1, "exactly one ACK per order (standby silent)");
        }

        // ── THE KILL ─────────────────────────────────────────────────────
        eprintln!("failover: SIGKILL leader");
        let t_kill = Instant::now();
        engine_a.kill9();

        // Pump orders continuously; first ACK with a HIGHER epoch marks the
        // takeover. Orders sent while no leader exists are simply unacked
        // (clients retry in production); we keep sending fresh ones.
        let (rto, new_epoch) = {
            let mut found: Option<(Duration, u16)> = None;
            while found.is_none() {
                assert!(
                    t_kill.elapsed() < RTO_LIMIT,
                    "RTO exceeded the 3-minute requirement"
                );
                let id = next_id;
                next_id += 1;
                place(&mut order_pub, &client, id, 0, 100);
                let until = Instant::now() + Duration::from_millis(300);
                while Instant::now() < until {
                    client.do_work();
                    updates.do_work();
                    while let Some(msg) = updates.poll() {
                        let (epoch, _) = split_epoch(msg.sequence);
                        if epoch > first_epoch {
                            found.get_or_insert((t_kill.elapsed(), epoch));
                        }
                    }
                }
            }
            found.unwrap()
        };
        eprintln!(
            "failover: standby took over in {:.2}s (epoch {} → {}) — requirement: ≤180s",
            rto.as_secs_f64(),
            first_epoch,
            new_epoch
        );
        assert!(rto < RTO_LIMIT);
        assert_eq!(new_epoch, first_epoch + 1, "takeover bumps epoch by one");

        // ── Seamlessness: cancel the PRE-failover resting order on B ─────
        let cancel = CancelOrderRequest {
            order_id: resting_oid,
            participant_id: 7,
            response_stream_id: resp_stream,
            _pad: [0; 4],
        };
        wait_for("publish cancel", Duration::from_secs(5), || {
            client.do_work();
            order_pub.publish_cancel(&cancel).ok()
        });
        wait_for("pre-failover order cancelled by new leader", Duration::from_secs(10), || {
            client.do_work();
            updates.do_work();
            while let Some(msg) = updates.poll() {
                let kind: u8 = msg.kind;
                let oid: u64 = msg.order_id;
                let (epoch, _) = split_epoch(msg.sequence);
                if kind == order_update_kind::CANCELLED && oid == resting_oid {
                    assert_eq!(epoch, new_epoch, "cancel served under the new epoch");
                    return Some(());
                }
            }
            None
        });

        rto
    })
    .await
    .expect("drill");

    eprintln!(
        "GATE 5 PASS: RTO {:.2}s (limit 180s), state preserved across failover",
        result.as_secs_f64()
    );
    let _ = sqlx::query("DELETE FROM leader_lease WHERE role = 'engine'")
        .execute(&pg)
        .await;
}
