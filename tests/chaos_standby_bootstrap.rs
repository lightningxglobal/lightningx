//! Standby-bootstrap drill (runbook §11.9, root-caused by §14.1) — a FRESH
//! machine with an EMPTY archive joins after the original engine machine
//! died, pulls the orders-stream journal from the peer archive with the
//! journal-replicate tool, and rebuilds the book from its LOCAL copy.
//!
//! Simulated topology: two ArchivingMediaDrivers = two machines.
//!   M1: archive + engine + order flow → engine SIGKILLed (machine "lost",
//!       archive survives — the realistic partial-loss case; total loss is
//!       covered by the peer archive being the source).
//!   M2: empty archive → journal-replicate(M1→M2) → engine starts against
//!       M2's archive ONLY → must answer a cancel of an order that was
//!       resting on M1 before the kill, with quantity intact.

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

const SRC_CONTROL_PORT: u16 = 18995;
const DST_CONTROL_PORT: u16 = 18997;
const SYMBOL: &str = "BTC_USDT";

fn spawn_engine(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_exchange-engine"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("SYMBOLS", SYMBOL)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("ENGINE_IDLE_SPINS", "1000")
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ProcGuard::spawn("engine", &mut cmd)
}

fn req(id: u64, side: u8, price: i64, qty: i64, stream: i32, tif: u8) -> NewOrderRequest {
    NewOrderRequest {
        client_order_id: id,
        participant_id: 11,
        price_ticks: price,
        quantity_lots: qty,
        side,
        time_in_force: tif,
        response_stream_id: stream,
        _pad: [0; 10],
        symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_machine_bootstraps_book_from_peer_archive() {
    let Some(jar) = jar_path() else {
        eprintln!("skip: aeron-all jar not found");
        return;
    };
    let Some(pg) = db::create_pool_sized(&pg_url(), 2).await.ok() else {
        eprintln!("skip: no PG");
        return;
    };
    db::run_migrations(&pg).await.expect("migrations");
    let Ok(m1) = start_archiving_driver(&jar, SRC_CONTROL_PORT) else {
        eprintln!("skip: cannot start M1 driver");
        return;
    };
    let Ok(m2) = start_archiving_driver(&jar, DST_CONTROL_PORT) else {
        eprintln!("skip: cannot start M2 driver");
        return;
    };
    let (m1_dir, m1_ctl) = (m1.aeron_dir.clone(), m1.control_channel.clone());
    let (m2_dir, m2_ctl) = (m2.aeron_dir.clone(), m2.control_channel.clone());

    tokio::task::spawn_blocking(move || {
        let resp_stream = order_update_stream_for_desk(0);
        let orders_stream = orders_stream_for_symbol(SYMBOL);

        // ── Phase 1: life on M1 — order flow, then the engine dies ───────
        let resting_oid = {
            let client = wait_for("M1 driver", Duration::from_secs(20), || {
                AeronClient::new(&m1_dir).ok().map(Arc::new)
            });
            let mut order_pub =
                DeskOrderPublisher::new(client.clone(), &orders_channel(), orders_stream)
                    .expect("M1 publisher");
            let mut updates = DeskOrderUpdateSubscriber::new(
                client.clone(),
                &order_update_channel(),
                resp_stream,
            )
            .expect("M1 updates");
            let mut engine = spawn_engine(&m1_dir, &m1_ctl).expect("spawn M1 engine");

            // Warm-up (IOC probes — zero book footprint).
            let mut next_id: u64 = 1;
            {
                let deadline = Instant::now() + Duration::from_secs(30);
                let mut warmed = false;
                while !warmed {
                    assert!(Instant::now() < deadline, "M1 engine never acked");
                    let r = req(next_id, 0, 100, 10, resp_stream, 1);
                    next_id += 1;
                    client.do_work();
                    let _ = order_pub.publish_new_order(&r);
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

            // Real flow: 60 resting GTC orders both sides.
            for i in 0..60u64 {
                let side = (i % 2) as u8;
                let price = if side == 0 { 90 + (i % 8) as i64 } else { 103 + (i % 8) as i64 };
                let r = req(next_id, side, price, 5 + (i % 9) as i64, resp_stream, 0);
                wait_for("publish", Duration::from_secs(5), || {
                    client.do_work();
                    order_pub.publish_new_order(&r).ok()
                });
                next_id += 1;
                std::thread::sleep(Duration::from_millis(2));
            }
            // One specifically tracked deep resting sell.
            let tracked = next_id;
            let r = req(tracked, 1, 9_999, 42, resp_stream, 0);
            wait_for("publish tracked", Duration::from_secs(5), || {
                client.do_work();
                order_pub.publish_new_order(&r).ok()
            });
            let oid = wait_for("tracked ACCEPTED", Duration::from_secs(10), || {
                client.do_work();
                updates.do_work();
                while let Some(msg) = updates.poll() {
                    let kind: u8 = msg.kind;
                    let coid: u64 = msg.client_order_id;
                    if kind == order_update_kind::ACCEPTED && coid == tracked {
                        let o: u64 = msg.order_id;
                        return Some(o);
                    }
                }
                None
            });
            std::thread::sleep(Duration::from_millis(500)); // recording flushes
            eprintln!("bootstrap: M1 book built, tracked order {oid} resting — killing M1 engine");
            engine.kill9();
            oid
        };

        // ── Phase 2: journal-replicate M1 → M2 (the new tool) ────────────
        let status = Command::new(env!("CARGO_BIN_EXE_journal-replicate"))
            .env("AERON_DIR", &m2_dir)
            .env("EXCHANGE_ARCHIVE_CONTROL", &m2_ctl)
            .env("SRC_ARCHIVE_CONTROL", &m1_ctl)
            .env("SYMBOLS", SYMBOL)
            .env("RUST_LOG", "info")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()
            .expect("run journal-replicate");
        assert!(status.status.success(), "journal-replicate must exit 0");
        eprintln!("bootstrap: replication M1→M2 complete");

        // ── Phase 3: fresh engine on M2, LOCAL archive only ──────────────
        let client2 = wait_for("M2 driver", Duration::from_secs(20), || {
            AeronClient::new(&m2_dir).ok().map(Arc::new)
        });
        let mut order_pub2 =
            DeskOrderPublisher::new(client2.clone(), &orders_channel(), orders_stream)
                .expect("M2 publisher");
        let mut updates2 =
            DeskOrderUpdateSubscriber::new(client2.clone(), &order_update_channel(), resp_stream)
                .expect("M2 updates");
        let _engine2 = spawn_engine(&m2_dir, &m2_ctl).expect("spawn M2 engine");

        // Wait until M2's engine finished replaying and is live (bogus-id
        // cancel echo probe).
        wait_for("M2 engine live after bootstrap replay", Duration::from_secs(60), || {
            let probe = CancelOrderRequest {
                order_id: 777_777_777,
                participant_id: 11,
                response_stream_id: resp_stream,
                _pad: [0; 4],
            };
            client2.do_work();
            let _ = order_pub2.publish_cancel(&probe);
            let until = Instant::now() + Duration::from_millis(400);
            while Instant::now() < until {
                client2.do_work();
                updates2.do_work();
                while let Some(msg) = updates2.poll() {
                    let kind: u8 = msg.kind;
                    let oid: u64 = msg.order_id;
                    if kind == order_update_kind::CANCELLED && oid == 777_777_777 {
                        return Some(());
                    }
                }
            }
            None
        });

        // ── The proof: cancel the order that rested on M1 before the kill.
        let cancel = CancelOrderRequest {
            order_id: resting_oid,
            participant_id: 11,
            response_stream_id: resp_stream,
            _pad: [0; 4],
        };
        wait_for("publish cancel", Duration::from_secs(5), || {
            client2.do_work();
            order_pub2.publish_cancel(&cancel).ok()
        });
        let qty = wait_for("M1-era order cancelled on M2", Duration::from_secs(10), || {
            client2.do_work();
            updates2.do_work();
            while let Some(msg) = updates2.poll() {
                let kind: u8 = msg.kind;
                let oid: u64 = msg.order_id;
                if kind == order_update_kind::CANCELLED && oid == resting_oid {
                    let q: f64 = msg.fill_qty;
                    return Some(q);
                }
            }
            None
        });
        assert!(
            (qty - 42.0 * 1e-6).abs() < 1e-12,
            "tracked order must rest on M2 with its full M1 quantity (got {qty})"
        );
        eprintln!(
            "BOOTSTRAP PASS: fresh machine rebuilt the book from a replicated journal — \
             M1-era order cancelled on M2 with quantity intact"
        );
    })
    .await
    .expect("drill");
    let _ = pg;
}
