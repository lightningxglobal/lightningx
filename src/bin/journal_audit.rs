//! journal-audit — three-way reconciliation: Archive journal ↔ PG
//! checkpoints ↔ PG row counts.
//!
//! Replays every persist-stream recording (read-only, onto a private
//! stream id) and reports, per publisher: frame count, seq range, gaps —
//! then compares against persist_checkpoints and the trades table.
//!
//! Usage (archive + PG must be reachable):
//!   EXCHANGE_ARCHIVE_CONTROL=aeron:udp?endpoint=localhost:8010 \
//!   DATABASE_URL=postgres://... AERON_DIR=/dev/shm/aeron journal-audit
//!
//! Exit code 0 = consistent; 1 = discrepancies found (details on stdout).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aeron_wrapper::AeronClient;
use lightning_exchange::db;
use lightning_exchange::desk::pg_store::load_checkpoints;
use lightning_exchange::transport::aeron_channels::{PERSIST_CHANNEL, PERSIST_STREAM, aeron_dir};
use lightning_exchange::transport::aeron_transport::PersistSubscriber;
use lightning_exchange::transport::journal::{
    JournalReplayer, JOURNAL_RESTART_JUMP, archive_config_from_env,
};
use lightning_exchange::transport::persist_event::PersistKind;

/// Private replay stream for audits — never collides with live (31) or
/// writer catch-up replays.
const AUDIT_REPLAY_STREAM: i32 = 32;

#[derive(Default, Debug)]
struct PubStats {
    frames: u64,
    trades: u64,
    min_seq: u64,
    max_seq: u64,
    gaps: u64,
    last_seq: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let Some(cfg) = archive_config_from_env() else {
        anyhow::bail!("EXCHANGE_ARCHIVE_CONTROL must be set");
    };
    let pg_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&pg_url, 2).await?;

    // Replay + statistics on a blocking thread (archive threading contract).
    let stats: HashMap<u16, PubStats> = tokio::task::spawn_blocking(move || {
        let client = Arc::new(AeronClient::new(&aeron_dir()).expect("AeronClient"));
        let mut replayer = JournalReplayer::connect(&client, &cfg).expect("archive connect");
        let recordings = replayer
            .recordings("ipc", PERSIST_STREAM)
            .expect("list recordings");
        println!("journal-audit: {} recording(s)", recordings.len());

        let mut stats: HashMap<u16, PubStats> = HashMap::new();
        for rec in &recordings {
            let Some(_r) = replayer
                .replay_bounded(rec, PERSIST_CHANNEL, AUDIT_REPLAY_STREAM)
                .expect("start replay")
            else {
                continue;
            };
            let mut sub =
                PersistSubscriber::new(client.clone(), PERSIST_CHANNEL, AUDIT_REPLAY_STREAM)
                    .expect("replay subscriber");
            loop {
                client.do_work();
                sub.do_work();
                let mut got = false;
                while let Some(frame) = sub.poll() {
                    got = true;
                    let publisher: u16 = frame.publisher_id;
                    let seq: u64 = frame.seq;
                    let e = stats.entry(publisher).or_default();
                    e.frames += 1;
                    if frame.kind() == Some(PersistKind::TradeInsert) {
                        e.trades += 1;
                    }
                    if seq != 0 {
                        if e.min_seq == 0 || seq < e.min_seq {
                            e.min_seq = seq;
                        }
                        if seq > e.max_seq {
                            e.max_seq = seq;
                        }
                        if e.last_seq != 0 && seq > e.last_seq + 1 {
                            // clock-seeded restarts produce huge jumps; only
                            // count plausible in-recording holes.
                            let jump = seq - e.last_seq;
                            if jump < JOURNAL_RESTART_JUMP {
                                e.gaps += jump - 1;
                            }
                        }
                        e.last_seq = seq;
                    }
                }
                if !got {
                    if sub.replay_image_closed() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        stats
    })
    .await?;

    let floors = load_checkpoints(&pg).await?;
    let mut inconsistent = false;

    for (publisher, s) in &stats {
        println!(
            "publisher {publisher}: frames={} trades={} seq=[{}..{}] gaps={}",
            s.frames, s.trades, s.min_seq, s.max_seq, s.gaps
        );
        match floors.get(publisher) {
            Some(&floor) if floor > s.max_seq => {
                println!(
                    "  !! checkpoint {floor} BEYOND journal max {} — journal truncated past \
                     the floor or checkpoint corrupt",
                    s.max_seq
                );
                inconsistent = true;
            }
            Some(&floor) if floor < s.max_seq => {
                println!(
                    "  consumer behind journal: floor={floor}, journal max={} \
                     ({} frame(s) pending catch-up — fine if writers are running)",
                    s.max_seq,
                    s.max_seq.saturating_sub(floor)
                );
            }
            Some(_) => println!("  checkpoint == journal max — fully consumed"),
            None => {
                println!("  !! no checkpoint for publisher {publisher}");
                inconsistent = true;
            }
        }
        if s.gaps > 0 {
            println!("  !! {} in-recording sequence hole(s)", s.gaps);
            inconsistent = true;
        }
    }

    let trades_in_pg: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades")
        .fetch_one(&pg)
        .await?;
    let journal_trades: u64 = stats.values().map(|s| s.trades).sum();
    println!("trades: journal={journal_trades} pg={trades_in_pg}");
    if (trades_in_pg as u64) < journal_trades {
        println!("  !! PG holds FEWER trades than the journal delivered");
        inconsistent = true;
    }

    if inconsistent {
        println!("RESULT: INCONSISTENT");
        std::process::exit(1);
    }
    println!("RESULT: consistent");
    Ok(())
}
