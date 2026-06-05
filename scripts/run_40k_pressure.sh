#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-/Users/alphawu/.cargo/global-target/release}"
DATABASE_URL="${DATABASE_URL:-postgres://user:password@localhost:5432/mydb}"
AERON_DIR="${AERON_DIR:-/tmp/aeron}"
TOKENS_CSV="${TOKENS_CSV:-/tmp/pressure_users_40k.csv}"
TOTAL_CONNS="${TOTAL_CONNS:-40000}"
DESK_COUNT="${DESK_COUNT:-4}"
TRACER_ENABLED="${TRACER_ENABLED:-0}"
CONNS_PER_SOURCE_IP="${CONNS_PER_SOURCE_IP:-15000}"
LOG_DIR="${LOG_DIR:-/tmp/lightning-${TOTAL_CONNS}-${DESK_COUNT}desk-$(date +%Y%m%d-%H%M%S)}"

ENGINE_BIN="${ENGINE_BIN:-$BIN_DIR/exchange-engine}"
DESK_BIN="${DESK_BIN:-$BIN_DIR/desk-server}"
PRESSURE_BIN="${PRESSURE_BIN:-$BIN_DIR/pressure-client}"

SOURCE_IPS=(127.0.0.2 127.0.0.3 127.0.0.4 127.0.0.5)
PORTS=(4003 4004 4005 4006)

if (( DESK_COUNT < 1 || DESK_COUNT > ${#PORTS[@]} )); then
  echo "DESK_COUNT must be between 1 and ${#PORTS[@]} (got $DESK_COUNT)" >&2
  exit 2
fi

for ip in "${SOURCE_IPS[@]}"; do
  if ! ifconfig lo0 | grep -q "inet ${ip} "; then
    echo "missing lo0 alias ${ip}; run: sudo ifconfig lo0 alias ${ip} up" >&2
    exit 2
  fi
done

if [[ ! -f "$TOKENS_CSV" ]]; then
  echo "missing token csv: $TOKENS_CSV" >&2
  exit 2
fi

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

# Kill any stale service processes from previous runs before starting fresh.
pkill -f "exchange-engine|desk-server|market-data-gateway|pressure-client" 2>/dev/null || true
sleep 1

psql "$DATABASE_URL" -c "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE status IN ('PENDING','TRADING');"

GATEWAY_BIN="${GATEWAY_BIN:-$BIN_DIR/market-data-gateway}"

env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
  RUST_LOG=warning TRACER_ENABLED=0 ENGINE_IDLE_SPINS=0 ORDER_UPDATE_STREAM_COUNT="$DESK_COUNT" \
  "$ENGINE_BIN" >"$LOG_DIR/engine.log" 2>&1 &
pids+=("$!")
sleep 2

# Start market-data-gateway alongside desk-servers (public market data: depth, ticker).
if [[ -x "$GATEWAY_BIN" ]]; then
  env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
    RUST_LOG=warning \
    "$GATEWAY_BIN" >"$LOG_DIR/market-gateway.log" 2>&1 &
  pids+=("$!")
fi

for ((i = 0; i < DESK_COUNT; i++)); do
  env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
    RUST_LOG=warning TRACER_ENABLED="$TRACER_ENABLED" DESK_SPIN=true \
    DESK_PORT="${PORTS[$i]}" DESK_ID="$i" \
    TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-2}" NOFILE_LIMIT=262144 \
    "$DESK_BIN" >"$LOG_DIR/desk-$i.log" 2>&1 &
  pids+=("$!")
done
# Give desk-servers extra time when tracer is on so they can register the scenario with beacon.
sleep "${DESK_WARMUP_S:-5}"

base_conns=$((TOTAL_CONNS / DESK_COUNT))
extra_conns=$((TOTAL_CONNS % DESK_COUNT))
user_offset=0
ip_index=0

for ((i = 0; i < DESK_COUNT; i++)); do
  conns=$base_conns
  if (( i < extra_conns )); then
    conns=$((conns + 1))
  fi

  ips_needed=$(((conns + CONNS_PER_SOURCE_IP - 1) / CONNS_PER_SOURCE_IP))
  if (( ip_index + ips_needed > ${#SOURCE_IPS[@]} )); then
    echo "not enough source IPs for ${TOTAL_CONNS}/${DESK_COUNT}; need ${ips_needed} more for desk $i" >&2
    exit 2
  fi
  source_ip="${SOURCE_IPS[$ip_index]}"
  for ((j = 1; j < ips_needed; j++)); do
    source_ip="${source_ip},${SOURCE_IPS[$((ip_index + j))]}"
  done
  ip_index=$((ip_index + ips_needed))

  env PRESSURE_TOKENS_CSV="$TOKENS_CSV" PRESSURE_USERS="$conns" \
    PRESSURE_USER_OFFSET=0 PRESSURE_OWNER_SHARD="$i" PRESSURE_OWNER_SHARD_COUNT="$DESK_COUNT" \
    PRESSURE_CONNS="$conns" \
    PRESSURE_DURATION_S="${PRESSURE_DURATION_S:-30}" \
    PRESSURE_RAMP_S="${PRESSURE_RAMP_S:-30}" \
    PRESSURE_OPS_PER_SEC="${PRESSURE_OPS_PER_SEC:-0.2}" \
    PRESSURE_BASE_URL="http://127.0.0.1:${PORTS[$i]}" \
    PRESSURE_SOURCE_IPS="$source_ip" PRESSURE_SYMBOL=BTC_USDT \
    PRESSURE_WORKERS="${PRESSURE_WORKERS:-2}" NOFILE_LIMIT=262144 \
    RUST_LOG=warning "$PRESSURE_BIN" >"$LOG_DIR/pressure-$i.log" 2>&1 &
  pids+=("$!")

  echo "pressure-$i: conns=$conns owner_shard=$i source_ips=$source_ip port=${PORTS[$i]}"
  user_offset=$((user_offset + conns))
done

# pids layout: [engine, gateway(optional), desk-0..N-1, pressure-0..N-1]
# Skip all long-running processes; wait only on pressure clients.
if [[ -x "$GATEWAY_BIN" ]]; then
  _pids_skip=$((DESK_COUNT + 2))  # engine + gateway + desks
else
  _pids_skip=$((DESK_COUNT + 1))  # engine + desks
fi
wait "${pids[@]:$_pids_skip}" || true

echo
echo "===== ${TOTAL_CONNS} CLIENT-SIDE LATENCY (${DESK_COUNT} desk) ====="
for ((i = 0; i < DESK_COUNT; i++)); do
  echo
  echo "----- pressure-$i -> ${PORTS[$i]} -----"
  sed -n '/final summary/,$p' "$LOG_DIR/pressure-$i.log"
done

# ── Beacon query (only when TRACER_ENABLED=1) ──────────────────────────────────
if [[ "$TRACER_ENABLED" == "1" ]]; then
  echo
  echo "── waiting 15s for beacon flush to VictoriaMetrics ──"
  sleep 15
  echo
  echo "===== SERVER-INTERNAL LATENCY (E2E: WsOrderRecv → WsResponseSent) ====="
  python3 - <<'PYEOF'
import urllib.request, json, urllib.parse, sys

BASE = "http://localhost:8428"

def qrange(promql):
    url = f"{BASE}/api/v1/query_range?query={urllib.parse.quote(promql)}&start=now-8m&end=now&step=10s"
    with urllib.request.urlopen(url) as r:
        return json.load(r)

def query_pcts(scenario, from_ms, to_ms):
    results_map = {}
    for pct in ['p50', 'p90', 'p99', 'p999', 'max']:
        q = f'latency_us{{percentile="{pct}",scenario="{scenario}",from_milestone="{from_ms}",to_milestone="{to_ms}"}}'
        try:
            d = qrange(q)
            results = d.get('data', {}).get('result', [])
            main = [r for r in results if 'outlier_category' not in r['metric'] and r['metric'].get('group') == '*_*_*_*']
            if main:
                vals = [float(v) for _, v in main[0].get('values', []) if float(v) > 0]
                results_map[pct] = sum(vals)/len(vals) if vals else None
            else:
                results_map[pct] = None
        except Exception:
            results_map[pct] = None
    return results_map

scenario = 'ExchangeOrderFlow'
SITE = 'ExchangeDeskServer'

pcts = query_pcts(scenario, f'{SITE}.OnWsOrderRecv', f'{SITE}.OnWsResponseSent')
p50  = pcts.get('p50')
p90  = pcts.get('p90')
p99  = pcts.get('p99')
pmax = pcts.get('max')
if p50 is not None:
    print(f"  E2E total  p50={p50:>7.1f}µs  p90={p90 or 0:>8.1f}µs  p99={p99 or 0:>8.1f}µs  max={pmax or 0:>8.1f}µs")
else:
    print(f"  E2E total  (no data)")

# Total count
q = f'latency_count{{scenario="{scenario}",from_milestone="{SITE}.OnWsOrderRecv",to_milestone="{SITE}.OnWsResponseSent"}}'
try:
    d = qrange(q)
    results = d.get('data', {}).get('result', [])
    main = [r for r in results if 'outlier_category' not in r['metric'] and r['metric'].get('group') == '*_*_*_*']
    if main:
        total = sum(float(v) for _, v in main[0].get('values', []))
        print(f"\n  Total E2E traces measured: {total:.0f}")
except Exception:
    pass
PYEOF
fi
