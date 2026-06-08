# Bug Fix Checklist

Generated from multi-agent adversarial review (2026-06-08).

Legend: 🔴 Critical (money-loss / data-corruption) · 🟠 High · 🟡 Medium · 🟢 Low

---

## Group A — Money / Accounts

- [x] 🔴 **A1** `settle_trade_atoms` double-debit on replay  
  `src/desk/account_repository.rs` — 4 account UPDATEs execute before `ON CONFLICT DO NOTHING` on trades. Replay skips the trade INSERT but still debits buyer / credits seller again.  
  **Fix**: wrap all 4 UPDATEs + trade INSERT in one transaction, gated on trade non-existence.  
  **Test**: call `settle_trade_atoms` twice with same (buy_order_id, sell_order_id); assert balances unchanged after second call.

- [x] 🟠 **A2** `confirm_withdrawal` / `fail_withdrawal` use plain `+` not `checked_add`  
  `src/desk/account_repository.rs` ~line 250, 300 — inconsistent with `request_withdrawal` which uses `checked_add`.  
  **Fix**: replace `amount + fee` with `amount.checked_add(fee).ok_or(...)`.  
  **Test**: unit test with near-`i64::MAX` values.

- [x] 🟠 **A3** `push_upsert_row` silently stores 0 atoms on `to_atoms()` failure  
  `src/desk/pg_store.rs` lines 723-725 — `to_atoms(quantity).unwrap_or(0)` commits wrong data without logging.  
  **Fix**: mirror `push_upsert` behavior: return `false` and increment a skip counter on conversion failure.  
  **Test**: unit test passing non-finite f64; assert row not written and skip counter incremented.

- [x] 🟡 **A4** Flip-margin integer truncation dust leak  
  `src/desk/risk/engine.rs:827` — `flip_margin = fill_margin_atoms * remaining_qty / fill_qty_lots` truncates; dust is neither credited nor tracked.  
  **Fix**: route truncation residue to available_margin (or assert zero for exact symbols).  
  **Test**: close+flip conservation test for BTC (scale=1_000_000); assert Σ(available + order + used) before == after.

- [x] 🟡 **A5** No operational sum-zero conservation SQL query  
  Conservation only verified in property tests, not in prod monitoring.  
  **Fix**: add `/api/admin/conservation` endpoint or periodic log that asserts `SUM(balance_atoms) == total_deposits - withdrawals - fees`.

---

## Group B — Risk / Liquidation / Funding

- [x] 🔴 **B1** IOC CANCELLED leaves account stuck in `Liquidating` forever  
  `src/bin/desk_server.rs:3553-3564` — CANCELLED handler releases order margin but does NOT re-arm status to `LiquidationPending`. `run_risk_tick` filter skips `Liquidating` accounts forever.  
  **Fix**: in CANCELLED branch, when `meta.liq_price_ticks != 0`, call `set_account_status_if(Liquidating → LiquidationPending)` mirroring the REJECTED branch (lines 3531-3537).  
  **Test**: integration test: place liq IOC against empty book (no bids at liq_price); assert status reverts to LiquidationPending within 1 tick.

- [x] 🔴 **B2** Stale-equity false liquidation window after `hydrate_from_pg`  
  `src/desk/risk/engine.rs` — `hydrate_from_pg` sets `unrealized_pnl=0`, `mark_price_ticks=entry`. First `run_risk_tick` before real mark arrives may wrongly trigger liquidation.  
  **Fix**: add `mark_received: bool` flag per account; skip liquidation check until `mark_received=true` after hydration.  
  **Test**: hydrate account with high-profit position, run risk tick before mark update; assert no liquidation triggered.

- [x] 🟠 **B3** `order_margin` underflow `.max(0)` clamp hides money on liq fill path  
  `src/desk/risk/engine.rs:507-509` — liquidation fills pass nonzero `fill_margin_atoms` but `order_margin` was never incremented for them; `.max(0)` silently absorbs the value.  
  **Fix**: for liquidation fills (detected by `meta.liq_price_ticks != 0`), skip the `order_margin` decrement since it was never reserved; route the `fill_margin_atoms` directly to the correct account fields.  
  **Test**: add liquidation-with-nonzero-margin to `swap_zero_sum.rs`; verify Σequity unchanged.

- [x] 🟠 **B4** `INDEX_SOURCES` empty → raw mid fallback with no clamp/freeze, no warning  
  `src/bin/desk_server.rs:3082-3084` — dev-mode default dangerous in production.  
  **Fix**: log `WARN` at startup if `INDEX_SOURCES` is empty; refuse to trigger liquidations when running without index sources (treat as mark-frozen).  
  **Test**: startup test with empty INDEX_SOURCES; assert liquidation blocked and warning emitted.

- [x] 🟠 **B5** Mark frozen indefinitely with no staleness alarm or halt  
  No upper bound on how long mark can remain frozen; deeply underwater positions never liquidated.  
  **Fix**: add `mark_frozen_since: Option<Instant>`; after configurable threshold (e.g. 60s) emit alarm metric and halt new liquidations.  
  **Test**: unit test: freeze mark for >threshold; assert alarm fired.

- [x] 🟡 **B6** `RISK_TIERS` parse errors silently disable leverage limits  
  `src/desk/risk/calc.rs:118-131` — invalid env entries skipped with no warning; empty Vec → `check_leverage_tier` returns `Ok(())` unconditionally.  
  **Fix**: log `ERROR` for each skipped tier; if resulting Vec is empty, log `WARN "leverage limits disabled"`.  
  **Test**: parse malformed tier string; assert warning logged and returned Vec is empty.

- [x] 🟡 **B7** `FUNDING_CLAMP_E9` / `FUNDING_INTEREST_E9` no bounds validation  
  `src/desk/funding.rs:50-59` — negative clamp or absurd interest rate accepted silently.  
  **Fix**: at startup assert `clamp_e9 >= 0` and `interest_e9` within sane range (e.g. ±1_000_000).  
  **Test**: startup with negative clamp; assert process exits with error.

---

## Group C — Journal / Snapshot

- [x] 🔴 **C1** `replay_from()` silent clamping when `from_position < recording.start_position`  
  `src/transport/journal.rs:237` — `let start = from_position.max(recording.start_position)` skips events in the gap without any error, producing diverged engine state.  
  **Fix**: return `Err` (or panic) when `from_position < recording.start_position`; add startup check that `retention_hours * 3600 > snapshot_interval_secs`.  
  **Test**: integration test: configure retention shorter than snapshot interval; assert engine refuses to start or returns error.

- [x] 🟠 **C2** `engine_snapshot.rs save()` non-transactional  
  `src/matching/engine_snapshot.rs:96-114` — `SELECT MAX(seq)+1` and `INSERT` are two separate queries; concurrent second engine hits unique-constraint violation.  
  **Fix**: replace with single `INSERT ... SELECT COALESCE(MAX(snapshot_seq),0)+1 FROM engine_snapshots WHERE symbol=$1` in one statement.  
  **Test**: unit test concurrent save() calls for same symbol; assert exactly one succeeds.

- [x] 🟢 **C3** Duplicated `2^40` restart-jump constant  
  `src/desk/pg_store.rs:264` and `src/bin/journal_audit.rs:94` — two bare literals.  
  **Fix**: define `pub const JOURNAL_RESTART_JUMP: u64 = 1 << 40` in `src/transport/journal.rs`; import in both places.

- [x] 🟢 **C4** `engine_journal_replay` test only checks top-100 book levels  
  `tests/engine_journal_replay.rs:297-307` — does not verify `next_order_id`, `trade_sequence`, full depth.  
  **Fix**: extend assertions to include order ID counter and trade sequence after replay.

- [x] 🟢 **C5** Soak test restart-time bound too loose  
  `tests/soak_engine.rs` — 60s bound passes even with full-genesis replay; liveness probe doesn't verify book state.  
  **Fix**: after restart, submit a known order and verify it matches against a pre-placed resting order (proves book state, not just Aeron liveness).

---

## Group D — Trigger Orders

- [x] 🔴 **D1** Half-injection orphan: desk crashes between PG INSERT and Aeron publish  
  `src/desk/trigger.rs` — `needs_reinjection(true, false)` returns `false`; stop-loss permanently lost.  
  **Fix**: add `pending_submission` status to orders table; transition to `submitted` only after Aeron publish succeeds; treat `pending_submission` as needing re-injection.  
  **Test**: simulate crash after INSERT, before publish; restart desk; assert order is re-injected.

- [x] 🟠 **D2** `default_for_side()` is stop-loss only but name implies general trigger  
  `src/desk/trigger.rs` — buy→Rising, sell→Falling; take-profit for a long needs Rising but calling `default_for_side(1)` gives Falling.  
  **Fix**: rename to `default_stop_loss_direction()` or add `TriggerKind` enum (StopLoss/TakeProfit) and compute correct direction from kind+side.

- [x] 🟡 **D3** `ExchangeConfig.set()` market halt not persisted to PG  
  `src/desk/exchange_config.rs` — in-memory only; desk crash auto-resumes halted market.  
  **Fix**: persist halt state to a `exchange_config` PG table; hydrate on startup.  
  **Test**: halt market, kill desk, restart; assert market still halted.

- [x] 🟡 **D4** Chaos test teardown missing `DELETE FROM trigger_orders`  
  `tests/chaos_trigger_fire.rs:371-390` — leftover `pending` rows pollute next test run's TriggerBook.  
  **Fix**: add `DELETE FROM trigger_orders WHERE user_id = ANY($1)` to teardown.

- [x] 🟢 **D5** PgWriteBatch keep-last dedup not tested for out-of-order delivery  
  Only tested in-order (seq=1 then seq=2). Out-of-order replay could overwrite newer state with older frame.  
  **Fix**: add test: push seq=2 then seq=1; assert seq=2 value wins.

---

## Group E — HA / Leader Election

- [x] 🔴 **E1** No local lease deadline timer — leader losing PG keeps publishing  
  `src/desk/leader.rs` — no independent `Instant`-based stop; split-brain write window bounded by sqlx query timeout.  
  **Fix**: record `last_renew_success: Instant`; in the matching loop, if `now > last_renew_success + ttl`, stop publishing and exit, without waiting for `try_acquire`.  
  **Test**: chaos test: partition leader from PG (iptables DROP, not SIGKILL); assert old leader stops publishing within `ttl + ε`.

- [x] 🟠 **E2** 16-bit epoch truncation silently breaks fencing after 65535 takeovers  
  `src/desk/leader.rs stamp_epoch` — `epoch as u64 & 0xFFFF`; PG epoch is unbounded i64.  
  **Fix**: `debug_assert!(epoch < 65536)` in `stamp_epoch`; in `try_acquire`, if epoch >= 60000, emit `WARN "epoch approaching u16 ceiling"`.  
  **Test**: unit test: stamp_epoch with epoch=65536; assert debug panic in debug build.

- [x] 🟠 **E3** Consumer-side epoch enforcement unverified  
  desk-server, pg-writer, redis-writer, WS all MUST call `split_epoch` and drop lower-epoch messages — this is the load-bearing assumption of the entire HA scheme.  
  **Fix**: audit each consumer; add `assert!(epoch >= current_epoch)` or silent drop with counter metric at each consumer's Aeron receive path.  
  **Audit result**: desk_server already enforces epoch at lines 3275–3300 via `split_epoch`/`m_fenced` on every ORDER_UPDATE message. PersistEvent stream (consumed by pg_writer/redis_writer) carries no epoch stamp — it is the already-filtered output of desk_server's epoch-guarded path, so those two consumers are transitively protected. Module docs added to both binaries.

- [x] 🟠 **E4** PG unavailable → standby never takes over (fail-closed undocumented)  
  `src/desk/leader.rs` — `try_acquire` returning `Err` just logs and loops; no takeover.  
  **Fix**: document explicitly as fail-closed design in module doc; add `WARN "PG unreachable, standby cannot promote"` metric; add chaos test asserting this behavior.

- [ ] 🟠 **E5** Chaos tests only SIGKILL leader, never partition from PG  
  The dangerous split-brain scenario (alive leader + lost PG) is never tested.  
  **Fix**: add `chaos_leader_pg_partition` test using `iptables -I OUTPUT -d <pg_host> -j DROP` (Linux) or TCP proxy kill; assert old leader stops within TTL and standby promotes.

- [x] 🟡 **E6** `current_epoch()` uses `Ordering::Relaxed`  
  No ordering barrier between "leader lost" `-1` store and matching thread's last publish.  
  **Fix**: use `Ordering::SeqCst` for the `-1` store in the lease-lost path; `Ordering::Acquire` in the matching thread's read.

- [x] 🟢 **E7** `role='engine'` hardcoded in binary and test with no shared constant  
  **Fix**: define `pub const LEADER_ROLE_ENGINE: &str = "engine"` in `src/desk/leader.rs`; import in test.

- [x] 🟢 **E8** `leader_lease.rs` integration test timing too tight for loaded CI  
  TTL=1s, sleep=1.3s — may spuriously fail under slow PG.  
  **Fix**: set TTL=500ms, sleep=1200ms for more margin.

---

## Group F — `for_symbol()` / Misc

- [x] 🟠 **F1** `for_symbol()` silently returns BTC params for unknown symbols  
  `src/desk/symbol_rules.rs` — wrong margin/notional calculations for any new unlisted symbol.  
  **Fix**: in non-test builds, `panic!("unknown symbol: {symbol}")` in the fallback arm (or return `Err`).  
  **Test**: unit test unknown symbol in non-test cfg; assert panic / error.

- [x] 🟢 **F2** `flush_accounts` last-write-wins upsert has no sequence number guard  
  `src/desk/pg_store.rs` — out-of-order Aeron replay can regress PG balance silently.  
  **Fix**: add `frame_seq` column to accounts table; upsert only when `EXCLUDED.frame_seq > accounts.frame_seq`.

- [x] 🟢 **F3** `seq==0` frames bypass `admit_seq` dedup entirely  
  `src/desk/pg_store.rs:247-249` — `if seq == 0 { return true }` — any frame published with seq=0 is applied on every replay without dedup.  
  **Fix**: change to `if seq == 0 { warn!("seq=0 frame — applying without dedup"); return true; }` at minimum; ideally reject seq=0 at the publisher.

---

## Summary

| Priority | Count | Groups |
|----------|-------|--------|
| 🔴 Critical | 6 | A1, B1, B2, C1, D1, E1 |
| 🟠 High | 13 | A2, A3, B3-B5, C2, D2-D3, E2-E5, F1 |
| 🟡 Medium | 5 | A4, A5, B6-B7, D4 |
| 🟢 Low | 8 | A... C3-C5, D5, E6-E8, F2-F3 |
| **Total** | **32** | |
