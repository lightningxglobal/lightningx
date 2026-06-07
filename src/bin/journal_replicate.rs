//! Standby-bootstrap tool: replicate journal recordings from a PEER
//! archive into the LOCAL archive (runbook §11.9).
//!
//! A machine joining mid-life has an empty local archive — its engine
//! cannot rebuild the book from local replay. This tool pulls every
//! recording of the relevant streams from the peer (whose recordings may
//! still be ACTIVE), bounded to the peer's current recorded position, and
//! waits until the local copies cover those positions. Afterwards the
//! local engine starts normally and replays from the LOCAL archive.
//!
//! Environment:
//!   AERON_DIR                    local media driver dir
//!   EXCHANGE_ARCHIVE_CONTROL     LOCAL (destination) archive control channel
//!   SRC_ARCHIVE_CONTROL          PEER (source) archive control channel
//!                                e.g. aeron:udp?endpoint=10.0.0.5:8010
//!   SRC_ARCHIVE_CONTROL_STREAM   peer control stream id (default 10)
//!   REPLICATE_STREAMS            comma-separated stream ids; default:
//!                                persist stream + orders streams derived
//!                                from SYMBOLS (the full recovery set)
//!   REPLICATE_TIMEOUT_SECS       per-recording completion deadline (600)
//!
//! Exit code: 0 = every recording replicated and verified; 1 = failure.
//! Idempotent: a source recording whose (session_id, stream_id) already
//! exists locally with coverage ≥ target is skipped — safe to re-run.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aeron_wrapper::AeronClient;
use lightning_exchange::transport::aeron_channels::{
    PERSIST_STREAM, aeron_dir, orders_stream_for_symbol,
};
use lightning_exchange::transport::journal::{JournalReplayer, archive_config_from_env};

fn streams_from_env() -> Vec<i32> {
    if let Ok(v) = std::env::var("REPLICATE_STREAMS") {
        let parsed: Vec<i32> = v
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        if !parsed.is_empty() {
            return parsed;
        }
    }
    // Default: everything an engine or writer needs to recover.
    let mut streams = vec![PERSIST_STREAM];
    if let Ok(symbols) = std::env::var("SYMBOLS") {
        for sym in symbols.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            streams.push(orders_stream_for_symbol(sym));
        }
    }
    streams.sort_unstable();
    streams.dedup();
    streams
}

fn main() {
    tracing_subscriber::fmt::init();

    let dst_cfg = archive_config_from_env()
        .expect("EXCHANGE_ARCHIVE_CONTROL must point at the LOCAL archive");
    let src_control = std::env::var("SRC_ARCHIVE_CONTROL")
        .expect("SRC_ARCHIVE_CONTROL must point at the PEER archive");
    let src_stream_id: i32 = std::env::var("SRC_ARCHIVE_CONTROL_STREAM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let timeout = Duration::from_secs(
        std::env::var("REPLICATE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(600),
    );
    let src_cfg = aeron_wrapper::ArchiveConfig {
        control_request_channel: src_control.clone(),
        control_request_stream_id: src_stream_id,
        ..aeron_wrapper::ArchiveConfig::default()
    };

    let client = Arc::new(AeronClient::new(&aeron_dir()).expect("AeronClient"));
    // Both control sessions live on this thread's conductor — contract ok.
    let mut src = JournalReplayer::connect(&client, &src_cfg).expect("connect peer archive");
    let mut dst = JournalReplayer::connect(&client, &dst_cfg).expect("connect local archive");

    let mut total = 0u32;
    let mut skipped = 0u32;
    let mut failed = 0u32;

    for stream in streams_from_env() {
        let src_recs = src.recordings("aeron", stream).expect("list peer recordings");
        tracing::info!("stream {stream}: {} recording(s) at peer", src_recs.len());
        for rec in src_recs {
            total += 1;
            let target = match src.archive_mut().max_recorded_position(rec.recording_id) {
                Ok(p) if p > rec.start_position => p,
                Ok(_) => {
                    skipped += 1;
                    tracing::info!("  recording {} empty — skip", rec.recording_id);
                    continue;
                }
                Err(e) => {
                    tracing::error!("  recording {}: max position: {e}", rec.recording_id);
                    failed += 1;
                    continue;
                }
            };

            // Idempotency: already covered locally?
            let covered = dst
                .recordings("aeron", stream)
                .unwrap_or_default()
                .into_iter()
                .filter(|d| d.session_id == rec.session_id)
                .any(|d| {
                    dst.archive_mut()
                        .max_recorded_position(d.recording_id)
                        .map(|p| p >= target)
                        .unwrap_or(false)
                });
            if covered {
                skipped += 1;
                tracing::info!(
                    "  recording {} (session {}) already covered locally — skip",
                    rec.recording_id,
                    rec.session_id
                );
                continue;
            }

            tracing::info!(
                "  replicating recording {} (session {}, {}..{target})",
                rec.recording_id,
                rec.session_id,
                rec.start_position
            );
            if let Err(e) = dst.archive_mut().replicate(
                rec.recording_id,
                &src_control,
                src_stream_id,
                Some(target),
            ) {
                tracing::error!("  replicate failed: {e}");
                failed += 1;
                continue;
            }

            // Wait for local coverage.
            let deadline = Instant::now() + timeout;
            let mut done = false;
            while Instant::now() < deadline {
                client.do_work();
                let ok = dst
                    .recordings("aeron", stream)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|d| d.session_id == rec.session_id)
                    .any(|d| {
                        dst.archive_mut()
                            .max_recorded_position(d.recording_id)
                            .map(|p| p >= target)
                            .unwrap_or(false)
                    });
                if ok {
                    done = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            if done {
                tracing::info!("  ✓ replicated to {target}");
            } else {
                tracing::error!("  ✗ timed out waiting for local coverage of {target}");
                failed += 1;
            }
        }
    }

    println!(
        "journal-replicate: {total} recording(s), {skipped} skipped (covered/empty), {failed} failed"
    );
    std::process::exit(if failed == 0 { 0 } else { 1 });
}
