#!/usr/bin/env bash
# 100K WS connection pressure test.
#
# Defaults to 2 desk-server processes on the local macOS test host because it
# gives the best accepted-order p50 in the current 100K single-machine shape.
# DESK_COUNT can be overridden for A/B tests:
#
#   DESK_COUNT=2 scripts/run_100k_pressure.sh
#   DESK_COUNT=3 scripts/run_100k_pressure.sh
#   DESK_COUNT=4 scripts/run_100k_pressure.sh
#   DESK_COUNT=5 scripts/run_100k_pressure.sh
#
# Required loopback aliases for the default 10 source IP pool:
#   for i in $(seq 2 11); do sudo ifconfig lo0 alias 127.0.0.$i up; done
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-/Users/alphawu/.cargo/global-target/release}"
DATABASE_URL="${DATABASE_URL:-postgres://user:password@localhost:5432/mydb}"
AERON_DIR="${AERON_DIR:-/tmp/aeron}"
TOKENS_CSV="${TOKENS_CSV:-/tmp/pressure_users_100k.csv}"
TOTAL_CONNS="${TOTAL_CONNS:-100000}"
DESK_COUNT="${DESK_COUNT:-2}"
TRACER_ENABLED="${TRACER_ENABLED:-0}"
DESK_SPIN="${DESK_SPIN:-false}"
CONNS_PER_SOURCE_IP="${CONNS_PER_SOURCE_IP:-15000}"
LOG_DIR="${LOG_DIR:-/tmp/lightning-${TOTAL_CONNS}-${DESK_COUNT}desk-$(date +%Y%m%d-%H%M%S)}"

ENGINE_BIN="${ENGINE_BIN:-$BIN_DIR/exchange-engine}"
DESK_BIN="${DESK_BIN:-$BIN_DIR/desk-server}"
PRESSURE_BIN="${PRESSURE_BIN:-$BIN_DIR/pressure-client}"

SOURCE_IP_POOL=(
  127.0.0.2 127.0.0.3 127.0.0.4 127.0.0.5 127.0.0.6
  127.0.0.7 127.0.0.8 127.0.0.9 127.0.0.10 127.0.0.11
)

if (( DESK_COUNT < 1 || DESK_COUNT > ${#SOURCE_IP_POOL[@]} )); then
  echo "DESK_COUNT must be between 1 and ${#SOURCE_IP_POOL[@]} (got $DESK_COUNT)" >&2
  exit 2
fi

if [[ ! -f "$TOKENS_CSV" ]]; then
  echo "missing token csv: $TOKENS_CSV" >&2
  exit 2
fi

token_rows=$(wc -l < "$TOKENS_CSV" | tr -d ' ')
if (( token_rows < TOTAL_CONNS )); then
  echo "token csv has $token_rows rows, need at least $TOTAL_CONNS: $TOKENS_CSV" >&2
  exit 2
fi

for ip in "${SOURCE_IP_POOL[@]}"; do
  if ! ifconfig lo0 | grep -q "inet ${ip} "; then
    echo "missing lo0 alias ${ip}; run: sudo ifconfig lo0 alias ${ip} up" >&2
    exit 2
  fi
done

mkdir -p "$LOG_DIR"
echo "logs: $LOG_DIR"
echo "shape: ${TOTAL_CONNS} conns across ${DESK_COUNT} desk-server processes  TRACER_ENABLED=${TRACER_ENABLED}"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

psql "$DATABASE_URL" -c \
  "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE status IN ('PENDING','TRADING');"

env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
  RUST_LOG=warning TRACER_ENABLED=0 ENGINE_IDLE_SPINS=0 ORDER_UPDATE_STREAM_COUNT="$DESK_COUNT" \
  "$ENGINE_BIN" >"$LOG_DIR/engine.log" 2>&1 &
pids+=("$!")
sleep 2

for ((i = 0; i < DESK_COUNT; i++)); do
  port=$((4003 + i))
  env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
    RUST_LOG=warning TRACER_ENABLED="$TRACER_ENABLED" DESK_SPIN="$DESK_SPIN" DESK_PORT="$port" DESK_ID="$i" \
    TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-2}" NOFILE_LIMIT=524288 \
    "$DESK_BIN" >"$LOG_DIR/desk-$i.log" 2>&1 &
  pids+=("$!")
done
sleep "${DESK_WARMUP_S:-8}"

base_conns=$((TOTAL_CONNS / DESK_COUNT))
extra_conns=$((TOTAL_CONNS % DESK_COUNT))
user_offset=0
ip_index=0

for ((i = 0; i < DESK_COUNT; i++)); do
  conns=$base_conns
  if (( i < extra_conns )); then
    conns=$((conns + 1))
  fi

  port=$((4003 + i))

  # macOS loopback usually has ~16K ephemeral ports per source IP.
  ips_needed=$(((conns + CONNS_PER_SOURCE_IP - 1) / CONNS_PER_SOURCE_IP))
  if (( ip_index + ips_needed > ${#SOURCE_IP_POOL[@]} )); then
    echo "not enough source IPs for ${TOTAL_CONNS}/${DESK_COUNT}; need ${ips_needed} more for desk $i" >&2
    exit 2
  fi
  source_ip="${SOURCE_IP_POOL[$ip_index]}"
  for ((j = 1; j < ips_needed; j++)); do
    source_ip="${source_ip},${SOURCE_IP_POOL[$((ip_index + j))]}"
  done
  ip_index=$((ip_index + ips_needed))

  env PRESSURE_TOKENS_CSV="$TOKENS_CSV" \
    PRESSURE_USERS="$conns" PRESSURE_USER_OFFSET="$user_offset" PRESSURE_CONNS="$conns" \
    PRESSURE_DURATION_S="${PRESSURE_DURATION_S:-30}" \
    PRESSURE_RAMP_S="${PRESSURE_RAMP_S:-60}" \
    PRESSURE_OPS_PER_SEC="${PRESSURE_OPS_PER_SEC:-0.2}" \
    PRESSURE_BASE_URL="http://127.0.0.1:${port}" \
    PRESSURE_SOURCE_IPS="$source_ip" PRESSURE_SYMBOL=BTC_USDT \
    PRESSURE_WORKERS="${PRESSURE_WORKERS:-3}" NOFILE_LIMIT=524288 \
    RUST_LOG=warning "$PRESSURE_BIN" >"$LOG_DIR/pressure-$i.log" 2>&1 &
  pids+=("$!")

  echo "pressure-$i: conns=$conns user_offset=$user_offset source_ips=$source_ip port=$port"
  user_offset=$((user_offset + conns))
done

wait "${pids[@]:$((DESK_COUNT + 1))}" || true

echo
echo "===== ${TOTAL_CONNS} CLIENT-SIDE LATENCY (${DESK_COUNT} desk) ====="
for ((i = 0; i < DESK_COUNT; i++)); do
  port=$((4003 + i))
  echo
  echo "----- pressure-$i -> ${port} -----"
  sed -n '/final summary/,$p' "$LOG_DIR/pressure-$i.log"
done

# ── Beacon query (only when TRACER_ENABLED=1) ──────────────────────────────────
if [[ "$TRACER_ENABLED" == "1" ]]; then
  echo
  echo "── waiting 15s for beacon flush to VictoriaMetrics ──"
  sleep 15
  echo
  echo "===== SERVER-INTERNAL LATENCY (beacon: OnWsOrderRecv → OnWsResponseSent) ====="
  python3 - <<'PYEOF'
import urllib.request, json, urllib.parse

BASE = "http://localhost:8428"

def qrange(promql):
    url = f"{BASE}/api/v1/query_range?query={urllib.parse.quote(promql)}&start=now-30m&end=now&step=10s"
    with urllib.request.urlopen(url) as r:
        return json.load(r)

scenario = 'ExchangeOrderFlow'
from_ms  = 'ExchangeDeskServer.OnWsOrderRecv'
to_ms    = 'ExchangeDeskServer.OnWsResponseSent'

for pct in ['p50', 'p90', 'p99', 'p999', 'max']:
    q = f'latency_us{{percentile="{pct}",scenario="{scenario}",from_milestone="{from_ms}",to_milestone="{to_ms}"}}'
    try:
        d = qrange(q)
    except Exception as e:
        print(f"  {pct}: query error: {e}")
        continue
    results = d.get('data', {}).get('result', [])
    main = [r for r in results if 'outlier_category' not in r['metric'] and r['metric'].get('group') == '*_*_*_*']
    if main:
        vals = [float(v) for _, v in main[0].get('values', []) if float(v) > 0]
        if vals:
            print(f"  {pct:6s}: avg={sum(vals)/len(vals):>8.1f} µs   peak={max(vals):>8.1f} µs")
        else:
            print(f"  {pct:6s}: (no data in window)")
    else:
        print(f"  {pct:6s}: (no series)")

q = f'latency_avg_us{{scenario="{scenario}",from_milestone="{from_ms}",to_milestone="{to_ms}"}}'
try:
    d = qrange(q)
    results = d.get('data', {}).get('result', [])
    main = [r for r in results if 'outlier_category' not in r['metric'] and r['metric'].get('group') == '*_*_*_*']
    if main:
        vals = [float(v) for _, v in main[0].get('values', []) if float(v) > 0]
        if vals:
            print(f"  {'avg':6s}: {sum(vals)/len(vals):>8.1f} µs")
except Exception:
    pass

q = f'latency_count{{scenario="{scenario}",from_milestone="{from_ms}",to_milestone="{to_ms}"}}'
try:
    d = qrange(q)
    results = d.get('data', {}).get('result', [])
    main = [r for r in results if 'outlier_category' not in r['metric'] and r['metric'].get('group') == '*_*_*_*']
    if main:
        total = sum(float(v) for _, v in main[0].get('values', []))
        print(f"  {'count':6s}: {total:.0f} traces")
except Exception:
    pass
PYEOF
fi
