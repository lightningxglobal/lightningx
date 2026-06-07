//! Phase-4 pg-writer:
//! Subscribe to the Aeron PersistEvent stream and batch-apply frames to
//! Postgres. Pairs with `redis-writer` (which applies the same stream to
//! Redis L1) so PG writes can be pulled out of the desk-server hot path.
//!
//! Environment:
//!   DATABASE_URL    postgres://user:password@127.0.0.1:5432/mydb
//!   AERON_DIR       /tmp/aeron (macOS default) or /dev/shm/aeron (Linux)
//!   PG_WRITER_BATCH        max frames buffered before forced flush (default 256)
//!   PG_WRITER_FLUSH_MS     time-based flush ceiling in ms        (default 50)
//!   PG_WRITER_MAX_CONNS    sqlx pool size                        (default 8)
//!
//! Runs alongside the existing desk-server DB worker — applies are
//! idempotent (INSERT ON CONFLICT / WHERE id = ANY) so dual-write is safe
//! during the cutover window. Once PR4b/c retire the desk-server PG path,
//! pg-writer becomes the sole writer to `orders`, `accounts`, `trades`.

use aeron_wrapper::AeronClient;
use anyhow::Context;
use crossbeam_queue::ArrayQueue;
use lightning_exchange::db;
use lightning_exchange::desk::pg_store::{PgWriteBatch, backfill_from_redis, load_checkpoints};
use lightning_exchange::desk::reconcile;
use lightning_exchange::transport::aeron_channels::{PERSIST_CHANNEL, PERSIST_STREAM, aeron_dir};
use lightning_exchange::transport::aeron_transport::PersistSubscriber;
use lightning_exchange::transport::persist_event::PersistFrame;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let pg_url = db::database_url_from_env().context("DATABASE_URL must be set")?;
    let max_conns: u32 = parse_env_u32("PG_WRITER_MAX_CONNS", 16);
    // Bigger batches amortize the per-flush PG round-trip cost. UNNEST
    // INSERT scales linearly per row, so 5 000-row batches take ~100-200 ms
    // on commodity PG vs ~20-50 ms for 256-row batches — but cut flush rate
    // from ~312/s to ~16/s, lifting effective throughput.
    let max_batch: usize = parse_env_u32("PG_WRITER_BATCH", 5_000) as usize;
    let flush_ms: u64 = parse_env_u32("PG_WRITER_FLUSH_MS", 50) as u64;
    // Aeron→PG bridge queue. Bounded so we can't OOM, but large enough
    // (~1 M frames × 144 B ≈ 144 MB) to absorb minutes of overload before
    // dropping. Sustained PG-can't-keep-up scenarios still drop, but the
    // Aeron client never times out because the polling thread doesn't
    // share its loop with PG `.await`.
    let queue_cap: usize = parse_env_u32("PG_WRITER_QUEUE", 5_000_000) as usize;
    // Account invariant sweep interval (hanging freezes, legacy/atoms
    // drift). Read-only SQL, runs in this writer process — never on the
    // order hot path. 0 disables.
    let reconcile_secs: u64 = parse_env_u32("PG_RECONCILE_SECS", 300) as u64;
    let backfill = std::env::var("BACKFILL_ON_START")
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    let oneshot_backfill = std::env::var("ONESHOT_BACKFILL")
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    info!(
        "pg-writer starting (pg={}, max_conns={}, max_batch={}, flush_ms={}, backfill={})",
        redacted(&pg_url),
        max_conns,
        max_batch,
        flush_ms,
        backfill,
    );

    // Durability-enforcing pool: every connection pins synchronous_commit=on
    // (fails fast at startup if the server can't honor it).
    let pg = db::create_pool_sized(&pg_url, max_conns)
        .await
        .context("connect PG")?;
    db::run_migrations(&pg).await.context("run migrations")?;

    // Optional one-shot backfill: any id present in Redis active_orders
    // but missing from PG is rebuilt from the Redis HASH and INSERTed
    // into PG. Closes the startup gap when pg-writer is brought up after
    // desk-server (Aeron stream subscriptions don't replay historical
    // frames). Idempotent — safe to run repeatedly.
    if backfill {
        info!("BACKFILL_ON_START set — reconciling PG from Redis");
        match redis::Client::open(redis_url.as_str()) {
            Ok(client) => match client.get_multiplexed_async_connection().await {
                Ok(mut conn) => {
                    let _: Result<String, _> = redis::cmd("PING").query_async(&mut conn).await;
                    match backfill_from_redis(&pg, &mut conn).await {
                        Ok(stats) => info!(
                            "backfill done: scanned={} missing={} backfilled={} skipped_decode={}",
                            stats.redis_orders_scanned,
                            stats.missing_in_pg,
                            stats.backfilled,
                            stats.skipped_decode,
                        ),
                        Err(e) => warn!("backfill failed: {e} — continuing anyway"),
                    }
                }
                Err(e) => warn!("Redis connect failed at {redis_url}: {e} — skipping backfill"),
            },
            Err(e) => warn!("Invalid REDIS_URL '{redis_url}': {e} — skipping backfill"),
        }
        if oneshot_backfill {
            info!("ONESHOT_BACKFILL set; exiting after backfill");
            return Ok(());
        }
    }

    // Metrics endpoint (Prometheus text; scraped by VictoriaMetrics).
    if let Ok(addr) = std::env::var("PG_WRITER_METRICS_ADDR") {
        lightning_exchange::metrics::spawn_metrics_listener(addr);
    }

    let dir = aeron_dir();
    info!("connecting Aeron client (dir={})", dir);

    // Bridge queue between Aeron polling (dedicated std::thread) and PG
    // flushing (tokio task). MUST be separated because flush_now().await
    // can park the tokio task for 100ms-2s during PG INSERT, and if that
    // happens on the same thread as Aeron polling, the Aeron client's
    // conductor timeout fires (10s) → entire client dies. We measured
    // pg-writer dying with 47s service interval at 40K conn × 1 op/s.
    let queue: Arc<ArrayQueue<PersistFrame>> = Arc::new(ArrayQueue::new(queue_cap));
    let dropped_pushes = Arc::new(AtomicU64::new(0));
    {
        let q = queue.clone();
        lightning_exchange::metrics::register_gauge("pgw_bridge_queue_depth", move || {
            q.len() as f64
        });
        let d = dropped_pushes.clone();
        lightning_exchange::metrics::register_gauge("pgw_bridge_dropped_total", move || {
            d.load(Ordering::Relaxed) as f64
        });
    }

    let queue_tx = queue.clone();
    let dropped_tx = dropped_pushes.clone();
    let aeron_dir_owned = dir.clone();
    std::thread::Builder::new()
        .name("pgw-recv".to_string())
        .spawn(move || {
            let aeron =
                Arc::new(AeronClient::new(&aeron_dir_owned).expect("AeronClient::new failed"));
            // Live subscription is created FIRST but not polled until the
            // journal replay (below) completes: frames arriving meanwhile
            // wait in its image buffer, so the replay→live hand-off has no
            // gap; the checkpoint floor drops the overlap.
            let mut sub = PersistSubscriber::new(aeron.clone(), PERSIST_CHANNEL, PERSIST_STREAM)
                .map_err(|e| anyhow::anyhow!("{e}"))
                .expect("create PersistSubscriber");
            info!(
                "[aeron] subscribed to persist stream (channel={}, stream={})",
                PERSIST_CHANNEL, PERSIST_STREAM
            );

            // Journal catch-up: when EXCHANGE_ARCHIVE_CONTROL is set, replay
            // every recording of the persist stream into the bridge queue
            // before going live. Duplicates are dropped by the checkpoint
            // floor; frames the writer never received (down/ring overflow)
            // are recovered. This thread drives its own client's conductor,
            // satisfying the archive threading contract.
            if let Some(cfg) = lightning_exchange::transport::journal::archive_config_from_env() {
                use lightning_exchange::transport::journal::{
                    JournalReplayer, PERSIST_REPLAY_STREAM,
                };
                let mut replayer = JournalReplayer::connect(&aeron, &cfg)
                    .expect("journal replay requested but archive connect failed");
                let recordings = replayer
                    .recordings("ipc", PERSIST_STREAM)
                    .expect("list journal recordings");
                info!("journal: {} recording(s) to replay", recordings.len());
                for rec in &recordings {
                    let Some(replay) = replayer
                        .replay_bounded(rec, PERSIST_CHANNEL, PERSIST_REPLAY_STREAM)
                        .expect("start journal replay")
                    else {
                        continue;
                    };
                    let mut replay_sub =
                        PersistSubscriber::new(aeron.clone(), PERSIST_CHANNEL, PERSIST_REPLAY_STREAM)
                            .map_err(|e| anyhow::anyhow!("{e}"))
                            .expect("create replay subscriber");
                    // Bounded replay: the archive closes the replay image at
                    // bounded_to — image-close + drained ring is the exact,
                    // event-driven completion signal (no quiet-period
                    // heuristic).
                    let mut replayed: u64 = 0;
                    loop {
                        aeron.do_work();
                        replay_sub.do_work();
                        let mut got = false;
                        while let Some(frame) = replay_sub.poll() {
                            got = true;
                            replayed += 1;
                            while queue_tx.push(frame).is_err() {
                                // During catch-up we must NOT drop: the whole
                                // point is recovering frames. Brief stall is
                                // fine, the flusher is draining concurrently.
                                std::thread::sleep(Duration::from_millis(1));
                            }
                        }
                        if !got && replay_sub.replay_image_closed() {
                            break;
                        }
                        if !got {
                            std::hint::spin_loop();
                        }
                    }
                    info!(
                        "journal: recording {} replayed {} frame(s)",
                        replay.recording_id, replayed
                    );
                }
                info!("journal: catch-up complete, switching to live stream");
            }
            loop {
                aeron.do_work();
                sub.do_work();
                let mut did_work = false;
                while let Some(frame) = sub.poll() {
                    did_work = true;
                    if queue_tx.push(frame).is_err() {
                        // Bridge queue full — PG flush is sustainably
                        // behind. Drop (rate-limited log). redis-writer is
                        // still applying so live state is preserved; PG
                        // can be backfilled later.
                        let n = dropped_tx.fetch_add(1, Ordering::Relaxed) + 1;
                        if n % 100_000 == 0 {
                            warn!("[aeron] bridge queue full — dropped {n} frames");
                        }
                    }
                }
                if !did_work {
                    std::hint::spin_loop();
                }
            }
        })
        .context("spawn pg-writer-aeron thread")?;

    let mut batch = PgWriteBatch::new();
    // Exactly-once: resume from committed checkpoints — frames at or below
    // the floor are duplicates from replay/restart and are dropped.
    match load_checkpoints(&pg).await {
        Ok(floors) => {
            if !floors.is_empty() {
                info!("persist checkpoints loaded: {:?}", floors);
            }
            batch.set_applied_floors(floors);
        }
        Err(e) => warn!("failed to load persist checkpoints: {e} — starting at zero"),
    }
    let mut last_flush = Instant::now();
    let mut total_applied: u64 = 0;
    let mut total_flushes: u64 = 0;
    let mut last_log = Instant::now();
    let mut last_reconcile = Instant::now();
    // Optional Redis connection for the cross-store reconcile sweep.
    let mut reconcile_redis: Option<redis::aio::MultiplexedConnection> =
        match redis::Client::open(redis_url.as_str()) {
            Ok(client) => match client.get_multiplexed_async_connection().await {
                Ok(conn) => Some(conn),
                Err(e) => {
                    warn!("reconcile: Redis unavailable ({e}) — pg↔redis sweep disabled");
                    None
                }
            },
            Err(e) => {
                warn!("reconcile: invalid REDIS_URL ({e}) — pg↔redis sweep disabled");
                None
            }
        };

    loop {
        let mut did_work = false;
        // Drain frames from the bridge queue.
        while let Some(frame) = queue.pop() {
            did_work = true;
            // S3.3 ordering: a funding settlement must see exactly the
            // positions as of its sequence point — flush everything
            // before it, give it its own batch/transaction, then flush
            // immediately so later fills can't slip underneath it.
            let is_funding = frame.kind()
                == Some(lightning_exchange::transport::persist_event::PersistKind::FundingSettled);
            if is_funding && batch.len() > 0 {
                match flush_now(&pg, &mut batch).await {
                    Ok(n) => {
                        total_applied += n as u64;
                        total_flushes += 1;
                        last_flush = Instant::now();
                    }
                    Err(e) => tracing::error!("flush before funding failed: {e}"),
                }
            }
            let _ = batch.push(&frame);
            if is_funding {
                match flush_now(&pg, &mut batch).await {
                    Ok(n) => {
                        total_applied += n as u64;
                        total_flushes += 1;
                        last_flush = Instant::now();
                    }
                    Err(e) => tracing::error!("funding settlement flush failed: {e}"),
                }
            }
            if batch.len() >= max_batch {
                match flush_now(&pg, &mut batch).await {
                    Ok(n) => {
                        total_applied += n as u64;
                        total_flushes += 1;
                        last_flush = Instant::now();
                    }
                    Err(e) => error!("flush (size-trigger) failed: {e}"),
                }
            }
        }

        // Time-based flush ceiling: even sparse streams land on disk fast.
        if !batch.is_empty() && last_flush.elapsed() >= Duration::from_millis(flush_ms) {
            match flush_now(&pg, &mut batch).await {
                Ok(n) => {
                    total_applied += n as u64;
                    total_flushes += 1;
                }
                Err(e) => error!("flush (time-trigger) failed: {e}"),
            }
            last_flush = Instant::now();
        }

        if last_log.elapsed() >= Duration::from_secs(30) {
            // Export the cumulative counters VictoriaMetrics alerts on.
            lightning_exchange::metrics::register_gauge("pgw_applied_total", {
                let v = total_applied;
                move || v as f64
            });
            lightning_exchange::metrics::register_gauge("pgw_checkpoint_dup_total", {
                let v = batch.duplicate_seq_frames;
                move || v as f64
            });
            lightning_exchange::metrics::register_gauge("pgw_seq_gap_total", {
                let v = batch.seq_gap_frames;
                move || v as f64
            });
            let sc = batch.skip_counts();
            info!(
                "pg-writer: applied={} flushes={} skipped={} bridge_dropped={} queue_depth={} \
                 [unknown_kind={} upsert_bad_status={} upsert_bad_ts={} upsert_empty_str={} \
                 fill_bad_status={} account_empty_asset={} trade_empty_symbol={} \
                 trade_bad_ts={} matching_empty_symbol={} matching_bad_ts={} decode_failed={}] \
                 ckpt[dup={} gap={} restarts={}]",
                total_applied,
                total_flushes,
                batch.skipped(),
                dropped_pushes.load(Ordering::Relaxed),
                queue.len(),
                sc.unknown_kind,
                sc.upsert_bad_status,
                sc.upsert_bad_timestamp,
                sc.upsert_empty_string,
                sc.fill_bad_status,
                sc.account_empty_asset,
                sc.trade_empty_symbol,
                sc.trade_bad_timestamp,
                sc.matching_empty_symbol,
                sc.matching_bad_timestamp,
                sc.payload_decode_failed,
                batch.duplicate_seq_frames,
                batch.seq_gap_frames,
                batch.publisher_restarts,
            );
            last_log = Instant::now();
        }

        // Periodic account invariant sweep (read-only; see desk::reconcile).
        if reconcile_secs > 0 && last_reconcile.elapsed() >= Duration::from_secs(reconcile_secs) {
            last_reconcile = Instant::now();
            // Cross-store (PG ↔ Redis L1) comparison: persistent nonzero
            // results across sweeps indicate a writer bug; single transient
            // hits are normal eventual-consistency noise.
            if let Some(conn) = &mut reconcile_redis {
                match reconcile::check_pg_redis_accounts(&pg, conn, 10, 1000).await {
                    Ok(x) if x.is_clean() => {
                        info!("reconcile: pg↔redis clean ({} compared)", x.compared);
                    }
                    Ok(x) => {
                        warn!(
                            "reconcile: pg↔redis divergence — compared={} missing_in_redis={} mismatched={}",
                            x.compared, x.missing_in_redis, x.mismatched_total,
                        );
                        for m in x.mismatched.iter().take(10) {
                            warn!(
                                "reconcile: pg↔redis user={} asset={} pg=({},{}) redis=({:?},{:?})",
                                m.user_id,
                                m.asset,
                                m.pg_balance_atoms,
                                m.pg_frozen_atoms,
                                m.redis_balance_atoms,
                                m.redis_frozen_atoms,
                            );
                        }
                    }
                    Err(e) => warn!("reconcile: pg↔redis sweep failed: {e}"),
                }
            }
            // S1.5: swap-position invariants ride the same sweep.
            match reconcile::check_position_invariants(&pg).await {
                Ok(report) if report.is_clean() => {}
                Ok(report) => {
                    for v in &report.net_violations {
                        tracing::error!(
                            "RECONCILE position net exposure: symbol={} net_lots={} (must be 0)",
                            v.symbol,
                            v.net_lots
                        );
                    }
                    for m in &report.margin_mismatches {
                        tracing::error!(
                            "RECONCILE margin mismatch: user={} account_used={} positions_sum={}",
                            m.user_id,
                            m.account_used_margin_atoms,
                            m.positions_margin_sum_atoms
                        );
                    }
                }
                Err(e) => tracing::warn!("position reconcile failed: {e}"),
            }
            match reconcile::check_account_invariants(&pg).await {
                Ok(report) if report.is_clean() => {
                    info!("reconcile: account invariants clean");
                }
                Ok(report) => {
                    lightning_exchange::metrics::counter("pgw_reconcile_violations_total").inc();
                    error!(
                        "reconcile: INVARIANT VIOLATIONS — hanging_frozen={} \
                         orders_drift={} trades_drift={} orders_overfill={}",
                        report.hanging_frozen_total,
                        report.orders_drift_total,
                        report.trades_drift_total,
                        report.orders_overfill_total,
                    );
                    if !report.orders_drift_ids.is_empty() {
                        error!("reconcile: orders drift ids={:?}", report.orders_drift_ids);
                    }
                    if !report.trades_drift_ids.is_empty() {
                        error!("reconcile: trades drift ids={:?}", report.trades_drift_ids);
                    }
                    if !report.orders_overfill_ids.is_empty() {
                        error!(
                            "reconcile: orders OVER-FILL ids={:?}",
                            report.orders_overfill_ids
                        );
                    }
                    for r in &report.hanging_frozen {
                        error!(
                            "reconcile: hanging freeze user={} asset={} frozen_atoms={}",
                            r.user_id, r.asset, r.frozen_atoms
                        );
                    }
                }
                Err(e) => warn!("reconcile sweep failed: {e}"),
            }
        }

        if !did_work {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

async fn flush_now(pg: &sqlx::PgPool, batch: &mut PgWriteBatch) -> anyhow::Result<usize> {
    if batch.is_empty() {
        return Ok(0);
    }
    let started = Instant::now();
    let pending = batch.len();
    let written = match batch.flush(pg).await {
        Ok(n) => n,
        Err(e) => {
            warn!(
                "pg flush failed after {:?} ({} frames in batch): {e} — retaining batch for retry",
                started.elapsed(),
                pending
            );
            return Err(e);
        }
    };
    let dt = started.elapsed();
    if dt > Duration::from_millis(500) {
        warn!(
            "slow pg flush: {} frames → {} rows in {:?}",
            pending, written, dt
        );
    }
    Ok(written)
}

fn parse_env_u32(key: &str, default: u32) -> u32 {
    match std::env::var(key) {
        Ok(v) => match v.parse() {
            Ok(n) => n,
            Err(e) => {
                warn!("invalid {key}='{v}' ({e}); using default {default}");
                default
            }
        },
        Err(_) => default,
    }
}

fn redacted(url: &str) -> String {
    if let Some(at) = url.rfind('@') {
        if let Some(scheme_end) = url.find("://") {
            return format!("{}://***@{}", &url[..scheme_end], &url[at + 1..]);
        }
    }
    url.to_string()
}
