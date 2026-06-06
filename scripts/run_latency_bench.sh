#!/usr/bin/env bash
# Dual-perspective latency benchmark:
#   1. Client-side:  pressure-client reports p50/p90/p99 of the full round-trip
#                    (WS send → first-response frame back on the socket).
#   2. Server-side:  beacon milestones MS_WS_ORDER_RECV → MS_WS_RESPONSE_SENT
#                    measure the pure server processing window (recv frame →
#                    write_frame() completes in write actor).
#
# Defaults: 1 desk-server, 2000 connections, 2 ops/sec per conn (~4000 ord/s).
# Adjust with env vars:
#   CONNS=2000 OPS_PER_SEC=2 DURATION_S=90 scripts/run_latency_bench.sh
#
# Prerequisites:
#   - aeronmd running (aeron:ipc at /tmp/aeron)
#   - beacon running  (http://localhost:8080, streams to VM at localhost:8428)
#   - pressure_users_100k.csv at /tmp/pressure_users_100k.csv
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-/Users/alphawu/.cargo/global-target/release}"
DATABASE_URL="${DATABASE_URL:-postgres://user:password@localhost:5432/mydb}"
AERON_DIR="${AERON_DIR:-/tmp/aeron}"
TOKENS_CSV="${TOKENS_CSV:-/tmp/pressure_users_100k.csv}"

CONNS="${CONNS:-2000}"
OPS_PER_SEC="${OPS_PER_SEC:-2}"
DURATION_S="${DURATION_S:-90}"
RAMP_S="${RAMP_S:-30}"
DESK_PORT="${DESK_PORT:-4010}"

ENGINE_BIN="${BIN_DIR}/exchange-engine"
DESK_BIN="${BIN_DIR}/desk-server"
PRESSURE_BIN="${BIN_DIR}/pressure-client"

LOG_DIR="/tmp/latency-bench-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$LOG_DIR"

echo "══════════════════════════════════════════════════════"
echo " Latency Benchmark"
echo "  conns=${CONNS}  ops/sec/conn=${OPS_PER_SEC}  duration=${DURATION_S}s  ramp=${RAMP_S}s"
echo "  total order rate ≈ $((CONNS * 2)) send/recv events/s (place + cancel cycle)"
echo "  logs: $LOG_DIR"
echo "══════════════════════════════════════════════════════"

# ── Prereq checks ─────────────────────────────────────────────────────────────
if [[ ! -f "$TOKENS_CSV" ]]; then
  echo "ERROR: token csv not found: $TOKENS_CSV" >&2; exit 2
fi
if [[ ! -f "$ENGINE_BIN" ]]; then
  echo "ERROR: exchange-engine not built: $ENGINE_BIN" >&2; exit 2
fi
if [[ ! -f "$DESK_BIN" ]]; then
  echo "ERROR: desk-server not built: $DESK_BIN" >&2; exit 2
fi
if [[ ! -f "$PRESSURE_BIN" ]]; then
  echo "ERROR: pressure-client not built: $PRESSURE_BIN" >&2; exit 2
fi

curl -sf http://localhost:8080/health >/dev/null 2>&1 \
  || { echo "ERROR: beacon not running at localhost:8080"; exit 2; }
curl -sf http://localhost:8428/health >/dev/null 2>&1 \
  || { echo "ERROR: VictoriaMetrics not running at localhost:8428"; exit 2; }

# ── Cleanup on exit ───────────────────────────────────────────────────────────
pids=()
cleanup() {
  echo; echo "── shutting down ──"
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

# ── Reset DB ──────────────────────────────────────────────────────────────────
psql "$DATABASE_URL" -c \
  "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE status IN ('PENDING','TRADING');" \
  >/dev/null 2>&1 || true

# ── Start exchange-engine ──────────────────────────────────────────────────────
echo "[1/3] starting exchange-engine..."
env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
  RUST_LOG=warning TRACER_ENABLED=0 ENGINE_IDLE_SPINS=0 \
  "$ENGINE_BIN" >"$LOG_DIR/engine.log" 2>&1 &
pids+=("$!")
sleep 2

# ── Start desk-server (TRACER_ENABLED=1 for beacon) ───────────────────────────
echo "[2/3] starting desk-server (TRACER_ENABLED=1, port=${DESK_PORT})..."
env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
  RUST_LOG=warning TRACER_ENABLED=1 DESK_SPIN=true \
  DESK_PORT="$DESK_PORT" DESK_ID=0 \
  TOKIO_WORKER_THREADS=4 NOFILE_LIMIT=65536 \
  "$DESK_BIN" >"$LOG_DIR/desk.log" 2>&1 &
pids+=("$!")

# Wait for desk-server to register scenario with beacon
echo "  waiting 6s for desk-server + beacon scenario registration..."
sleep 6

SCENARIO_COUNT=$(curl -sf http://localhost:8080/health | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print(d['scenarios_registered'])" 2>/dev/null || echo "?")
echo "  beacon scenarios registered: ${SCENARIO_COUNT}"

# ── Run pressure client ────────────────────────────────────────────────────────
echo "[3/3] starting pressure-client (${CONNS} conns, ${OPS_PER_SEC} ops/sec, ${DURATION_S}s)..."
env PRESSURE_TOKENS_CSV="$TOKENS_CSV" \
  PRESSURE_USERS="$CONNS" PRESSURE_CONNS="$CONNS" \
  PRESSURE_OPS_PER_SEC="$OPS_PER_SEC" \
  PRESSURE_DURATION_S="$DURATION_S" \
  PRESSURE_RAMP_S="$RAMP_S" \
  PRESSURE_BASE_URL="http://127.0.0.1:${DESK_PORT}" \
  PRESSURE_SYMBOL=BTC_USDT \
  PRESSURE_WORKERS=4 NOFILE_LIMIT=65536 \
  RUST_LOG=warning "$PRESSURE_BIN" >"$LOG_DIR/pressure.log" 2>&1

# ── Client-side results ────────────────────────────────────────────────────────
echo
echo "══════════════════════════════════════════════════════"
echo " CLIENT-SIDE LATENCY  (loopback RTT, incl. network stack)"
echo "══════════════════════════════════════════════════════"
sed -n '/final summary/,$p' "$LOG_DIR/pressure.log"

# ── Wait for beacon flush (10s interval) ──────────────────────────────────────
echo
echo "── waiting 15s for beacon to flush metrics to VictoriaMetrics ──"
sleep 15

# ── Server-side beacon results ─────────────────────────────────────────────────
echo
echo "══════════════════════════════════════════════════════"
echo " SERVER-INTERNAL LATENCY  (beacon: OnWsOrderRecv → OnWsResponseSent)"
echo "══════════════════════════════════════════════════════"

VM_QUERY() {
  local pct="$1"
  curl -sg "http://localhost:8428/api/v1/query?query=latency_us%7Bpercentile%3D%22${pct}%22%2Cscenario%3D%22ExchangeDeskServer%22%7D" \
  | python3 -c "
import json, sys
d = json.load(sys.stdin)
results = d.get('data', {}).get('result', [])
if not results:
    print('  (no data)')
    sys.exit(0)
for r in results:
    m = r['metric']
    v = float(r['value'][1])
    print(f'  {pct}: {v:.1f} µs  [{m.get(\"from_milestone\",\"?\")} → {m.get(\"to_milestone\",\"?\")}]  group={m.get(\"group\",\"?\")}')
"
}

for pct in p50 p90 p99 p999 max; do
  VM_QUERY "$pct"
done

# Show count too
curl -sg "http://localhost:8428/api/v1/query?query=latency_count%7Bscenario%3D%22ExchangeDeskServer%22%7D" \
| python3 -c "
import json, sys
d = json.load(sys.stdin)
results = d.get('data', {}).get('result', [])
if results:
    total = sum(float(r['value'][1]) for r in results)
    print(f'  samples: {total:.0f}')
" 2>/dev/null || true

echo
echo "══════════════════════════════════════════════════════"
echo " CHECKPOINT STATS (beacon health)"
echo "══════════════════════════════════════════════════════"
curl -sf http://localhost:8080/health | python3 -c \
  "import json,sys; d=json.load(sys.stdin); [print(f'  {k}: {v}') for k,v in d.items()]" 2>/dev/null || true

echo
echo "Logs saved to: $LOG_DIR"
