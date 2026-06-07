//! Persist-stream journaling via Aeron Archive.
//!
//! The persist stream (desk → pg/redis writers) already carries
//! (publisher_id, seq) and the writers checkpoint exactly-once — but a frame
//! the writers never RECEIVED (ring overflow, writer down while desk kept
//! publishing) was still lost forever. With the stream recorded by an
//! ArchivingMediaDriver, a restarting writer first replays the recording and
//! relies on its checkpoint floor to deduplicate: replaying any prefix is
//! safe, missing frames are recovered, duplicates are dropped.
//!
//! Opt-in: set EXCHANGE_ARCHIVE_CONTROL (e.g.
//! "aeron:udp?endpoint=localhost:8010") on both desk-server (records) and
//! pg-writer (replays). Unset → everything behaves exactly as before.
//!
//! Threading contract (see aeron-wrapper archive docs): all archive control
//! calls happen on the thread that drives `AeronClient::do_work` — in both
//! integration points that is the dedicated Aeron poll/drain thread.
//!
//! Durability note: "recorded" means fsynced only when the archive runs
//! with `aeron.archive.file.sync.level >= 1`; set 2 for data+metadata.
//! Retention: stopped recordings older than EXCHANGE_JOURNAL_RETENTION_HOURS
//! are purged by the recording side (see JournalRecorder::purge_stopped_before).
//! 0/unset keeps everything. NOTE: replay-based recovery reaches back only
//! as far as retention — pair the horizon with your PG backup policy.

use std::sync::Arc;

use aeron_wrapper::{
    AeronArchive, AeronClient, ArchiveConfig, RecordingSummary, ReplayParams, SourceLocation,
};

/// Where journal replay traffic lands so it never collides with the live
/// persist stream id.
pub const PERSIST_REPLAY_STREAM: i32 = 31;

/// Replayed engine-input (orders) traffic lands on
/// `orders_stream + ORDERS_REPLAY_STREAM_OFFSET` — far above the live
/// per-symbol order streams (base 10 + index) and the persist streams.
pub const ORDERS_REPLAY_STREAM_OFFSET: i32 = 1000;

/// Env var holding the archive control-request channel. Unset = disabled.
pub const ARCHIVE_CONTROL_ENV: &str = "EXCHANGE_ARCHIVE_CONTROL";

pub fn archive_config_from_env() -> Option<ArchiveConfig> {
    let control = std::env::var(ARCHIVE_CONTROL_ENV).ok()?;
    if control.is_empty() {
        return None;
    }
    Some(ArchiveConfig {
        control_request_channel: control,
        ..ArchiveConfig::default()
    })
}

/// Records one channel/stream on the archive. Held by the publishing side
/// (desk persist drain thread) for the lifetime of the process.
pub struct JournalRecorder {
    archive: AeronArchive,
    subscription_id: i64,
}

impl JournalRecorder {
    /// Connect and start recording. Call from the thread that drives
    /// `client.do_work()`.
    pub fn start(
        client: &Arc<AeronClient>,
        config: &ArchiveConfig,
        channel: &str,
        stream_id: i32,
    ) -> Result<Self, String> {
        let mut archive =
            AeronArchive::connect(client, config).map_err(|e| format!("archive connect: {e}"))?;
        // A recording subscription SURVIVES its client: after a process
        // restart the archive still records this channel/stream (each new
        // publication session becomes a new recording). "recording exists"
        // therefore means the goal is already achieved — not an error.
        let subscription_id = match archive.start_recording(
            channel,
            stream_id,
            SourceLocation::Local,
            false,
        ) {
            Ok(id) => id,
            Err(e) if e.to_string().contains("recording exists") => {
                tracing::info!(
                    "journal: recording subscription for {channel} stream {stream_id} \
                     already active (previous lifetime) — reusing"
                );
                -1
            }
            Err(e) => return Err(format!("start_recording: {e}")),
        };
        tracing::info!(
            "journal: recording {channel} stream {stream_id} (subscription {subscription_id})"
        );
        Ok(Self {
            archive,
            subscription_id,
        })
    }

    pub fn subscription_id(&self) -> i64 {
        self.subscription_id
    }

    /// Purge STOPPED recordings of `channel_fragment`/`stream_id` whose stop
    /// timestamp is older than `cutoff_epoch_ms`. Active recordings (this
    /// lifetime's) are never touched. Returns (purged, segments_deleted).
    /// Call from the recording thread on a slow cadence.
    pub fn purge_stopped_before(
        &mut self,
        channel_fragment: &str,
        stream_id: i32,
        cutoff_epoch_ms: i64,
    ) -> Result<(usize, i64), String> {
        let recordings = self
            .archive
            .list_recordings_for_uri(channel_fragment, stream_id)
            .map_err(|e| format!("list recordings: {e}"))?;
        let mut purged = 0usize;
        let mut segments = 0i64;
        for rec in &recordings {
            let Some(stopped_at) = rec.stopped_at_ms() else {
                continue; // active
            };
            if stopped_at >= cutoff_epoch_ms {
                continue;
            }
            match self.archive.purge_recording(rec.recording_id) {
                Ok(n) => {
                    purged += 1;
                    segments += n;
                    tracing::info!(
                        "journal: purged recording {} ({} segment(s), stopped at {})",
                        rec.recording_id, n, stopped_at
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "journal: purge of recording {} failed: {e}",
                        rec.recording_id
                    );
                }
            }
        }
        Ok((purged, segments))
    }
}

/// Retention horizon from EXCHANGE_JOURNAL_RETENTION_HOURS (0/unset = keep
/// everything).
pub fn retention_hours_from_env() -> Option<u64> {
    std::env::var("EXCHANGE_JOURNAL_RETENTION_HOURS")
        .ok()?
        .parse::<u64>()
        .ok()
        .filter(|&h| h > 0)
}

/// One bounded replay in flight.
pub struct JournalReplay {
    pub recording_id: i64,
    pub replay_session_id: i64,
    /// Recorded byte-position the replay was bounded to; consuming the
    /// replay stream until it dries up re-delivers exactly this prefix.
    pub bounded_to: i64,
}

/// Journal consumer: lists and replays every recording of the persist
/// stream. A publisher restart creates a new session → a new recording, so
/// a complete catch-up replays ALL matching recordings in recording-id
/// (creation) order; the consumer's checkpoint floor drops duplicates.
///
/// Correct gap-free consumption order (without replay-merge):
/// 1. create the LIVE subscription first but do NOT poll it yet — frames
///    arriving during the replay wait in its image buffer;
/// 2. for each recording in order: replay_bounded + consume to dry;
/// 3. start polling the live subscription. Overlap between replay tail and
///    live head is dropped by the checkpoint floor.
pub struct JournalReplayer {
    archive: AeronArchive,
}

impl JournalReplayer {
    /// Call from the thread that drives `client.do_work()`.
    pub fn connect(client: &Arc<AeronClient>, config: &ArchiveConfig) -> Result<Self, String> {
        let archive =
            AeronArchive::connect(client, config).map_err(|e| format!("archive connect: {e}"))?;
        Ok(Self { archive })
    }

    /// All recordings of `channel_fragment`/`stream_id`, oldest first.
    pub fn recordings(
        &mut self,
        channel_fragment: &str,
        stream_id: i32,
    ) -> Result<Vec<RecordingSummary>, String> {
        self.archive
            .list_recordings_for_uri(channel_fragment, stream_id)
            .map_err(|e| format!("list recordings: {e}"))
    }

    /// Start a bounded full replay of one recording onto `replay_stream_id`.
    /// Ok(None) for an empty recording.
    pub fn replay_bounded(
        &mut self,
        recording: &RecordingSummary,
        replay_channel: &str,
        replay_stream_id: i32,
    ) -> Result<Option<JournalReplay>, String> {
        let bounded_to = self
            .archive
            .max_recorded_position(recording.recording_id)
            .map_err(|e| format!("max recorded position: {e}"))?;
        if bounded_to <= recording.start_position {
            return Ok(None);
        }
        let replay_session_id = self
            .archive
            .start_replay(
                recording.recording_id,
                replay_channel,
                replay_stream_id,
                ReplayParams {
                    position: Some(recording.start_position),
                    length: Some(bounded_to - recording.start_position),
                },
            )
            .map_err(|e| format!("start_replay: {e}"))?;
        tracing::info!(
            "journal: replaying recording {} ({}..{bounded_to}) onto stream {replay_stream_id}",
            recording.recording_id,
            recording.start_position,
        );
        Ok(Some(JournalReplay {
            recording_id: recording.recording_id,
            replay_session_id,
            bounded_to,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_config_gate() {
        // SAFETY: test-local env mutation; no other test reads this var.
        unsafe {
            std::env::remove_var(ARCHIVE_CONTROL_ENV);
        }
        assert!(archive_config_from_env().is_none(), "unset → disabled");
        unsafe {
            std::env::set_var(ARCHIVE_CONTROL_ENV, "");
        }
        assert!(archive_config_from_env().is_none(), "empty → disabled");
        unsafe {
            std::env::set_var(ARCHIVE_CONTROL_ENV, "aeron:udp?endpoint=localhost:9010");
        }
        let cfg = archive_config_from_env().expect("set → enabled");
        assert!(cfg.control_request_channel.contains("9010"));
        unsafe {
            std::env::remove_var(ARCHIVE_CONTROL_ENV);
        }
    }

    #[test]
    fn replay_stream_does_not_collide_with_live() {
        assert_ne!(
            PERSIST_REPLAY_STREAM,
            crate::transport::aeron_channels::PERSIST_STREAM
        );
    }
}
