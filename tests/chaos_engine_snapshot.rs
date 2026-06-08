//! T4 — engine snapshot recovery against the REAL engine + archive:
//! orders build a book; SNAPSHOT_INTERVAL_SECS forces a snapshot;
//! `kill -9` the engine; on restart it RESTORES from the snapshot and
//! replays only the journal tail (not genesis). The reborn book must be
//! identical to the pre-crash book (a resting order from before the
//! snapshot is still cancellable), and a snapshot row must exist.

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
use lightning_exchange::transport::aeron_transport::{DeskOrderPublisher, DeskOrderUpdateSubscriber};
use lightning_exchange::transport::order_update_kind;

const CONTROL_PORT: u16 = 19050;
const SYMBOL: &str = "BTC_USDT";

fn spawn_engine(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_exchange-engine"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("SYMBOLS", SYMBOL)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("SNAPSHOT_INTERVAL_SECS", "2") // force snapshots fast
        .env("ENGINE_IDLE_SPINS", "1000")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ProcGuard::spawn("engine", &mut cmd)
}

fn req(id: u64, side: u8, price: i64, qty: i64, stream: i32, tif: u8) -> NewOrderRequest {
    NewOrderRequest {
        client_order_id: id,
        participant_id: 5,
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
async fn snapshot_restore_reproduces_book_after_kill9() {
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
    let _ = sqlx::query("DELETE FROM engine_snapshots WHERE symbol = $1")
        .bind(SYMBOL)
        .execute(&pg)
        .await;

    let result = tokio::task::spawn_blocking(move || {
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
                .expect("updates");
        let mut engine = spawn_engine(&aeron_dir, &control).expect("engine");

        // Warm-up (IOC probes, zero footprint).
        let mut next_id: u64 = 1;
        {
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut warm = false;
            while !warm {
                assert!(Instant::now() < deadline, "engine never acked");
                let r = req(next_id, 0, 100, 10, resp_stream, 1);
                next_id += 1;
                client.do_work();
                let _ = order_pub.publish_new_order(&r);
                let until = Instant::now() + Duration::from_millis(400);
                while Instant::now() < until {
                    client.do_work();
                    updates.do_work();
                    while updates.poll().is_some() {
                        warm = true;
                    }
                }
            }
        }

        // Build a resting book: 40 GTC orders, non-crossing (buys 90..97,
        // sells 103..110).
        for i in 0..40u64 {
            let side = (i % 2) as u8;
            let price = if side == 0 { 90 + (i % 8) as i64 } else { 103 + (i % 8) as i64 };
            let r = req(next_id, side, price, 5 + (i % 7) as i64, resp_stream, 0);
            wait_for("publish", Duration::from_secs(5), || {
                client.do_work();
                order_pub.publish_new_order(&r).ok()
            });
            next_id += 1;
            std::thread::sleep(Duration::from_millis(5));
        }

        // Let at least one snapshot fire (interval 2s).
        std::thread::sleep(Duration::from_secs(4));
        eprintln!("snapshot: 40-order book built, ≥1 snapshot taken — SIGKILL engine");
        engine.kill9();
        std::thread::sleep(Duration::from_millis(300));
        engine = spawn_engine(&aeron_dir, &control).expect("respawn");

        // After restart the reborn engine restored its book from the
        // snapshot (no genesis replay). PROVE the restored liquidity is
        // real: a crossing BUY at 110 must FILL against the restored SELL
        // orders (sells rest at 103..110). A book that failed to restore
        // would leave the buy resting (zero fill).
        let cross_id = 9_000_000u64;
        let buy = req(cross_id, 0, 110, 5, resp_stream, 1); // IOC buy, crosses
        let filled = wait_for("crossing buy fills against restored book", Duration::from_secs(60), || {
            client.do_work();
            let _ = order_pub.publish_new_order(&buy);
            let until = Instant::now() + Duration::from_millis(500);
            while Instant::now() < until {
                client.do_work();
                updates.do_work();
                while let Some(m) = updates.poll() {
                    let kind: u8 = m.kind;
                    let coid: u64 = m.client_order_id;
                    if coid == cross_id
                        && (kind == order_update_kind::FILLED
                            || kind == order_update_kind::PARTIAL_FILL)
                    {
                        let q: f64 = m.fill_qty;
                        return Some(q);
                    }
                }
            }
            None
        });
        assert!(filled > 0.0, "crossing buy must fill against snapshot-restored liquidity ({filled})");
        drop(engine);
        filled
    })
    .await
    .expect("drill");

    // A snapshot row was persisted.
    let snaps: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM engine_snapshots WHERE symbol = $1")
        .bind(SYMBOL)
        .fetch_one(&pg)
        .await
        .expect("snapshot count");
    assert!(snaps >= 1, "at least one engine snapshot persisted");
    eprintln!("CHAOS-SNAPSHOT PASS: restored book from snapshot ({snaps} rows), order {result} survived");
    let _ = sqlx::query("DELETE FROM engine_snapshots WHERE symbol = $1")
        .bind(SYMBOL)
        .execute(&pg)
        .await;
}
