#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-/Users/alphawu/.cargo/global-target/release}"
DATABASE_URL="${DATABASE_URL:-postgres://user:password@localhost:5432/mydb}"
AERON_DIR="${AERON_DIR:-/tmp/aeron}"
TOKENS_CSV="${TOKENS_CSV:-/tmp/pressure_users_40k.csv}"
TOTAL_CONNS="${TOTAL_CONNS:-40000}"
DESK_COUNT="${DESK_COUNT:-4}"
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
echo "shape: ${TOTAL_CONNS} conns across ${DESK_COUNT} desk-server processes"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

psql "$DATABASE_URL" -c "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE status IN ('PENDING','TRADING');"

env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
  RUST_LOG=warning TRACER_ENABLED=0 ENGINE_IDLE_SPINS=0 \
  "$ENGINE_BIN" >"$LOG_DIR/engine.log" 2>&1 &
pids+=("$!")
sleep 2

for ((i = 0; i < DESK_COUNT; i++)); do
  env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
    RUST_LOG=warning TRACER_ENABLED=0 DESK_PORT="${PORTS[$i]}" DESK_ID="$i" \
    TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-2}" NOFILE_LIMIT=262144 \
    "$DESK_BIN" >"$LOG_DIR/desk-$i.log" 2>&1 &
  pids+=("$!")
done
sleep 5

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
    PRESSURE_USER_OFFSET="$user_offset" PRESSURE_CONNS="$conns" \
    PRESSURE_DURATION_S="${PRESSURE_DURATION_S:-30}" \
    PRESSURE_RAMP_S="${PRESSURE_RAMP_S:-30}" \
    PRESSURE_OPS_PER_SEC="${PRESSURE_OPS_PER_SEC:-0.2}" \
    PRESSURE_BASE_URL="http://127.0.0.1:${PORTS[$i]}" \
    PRESSURE_SOURCE_IPS="$source_ip" PRESSURE_SYMBOL=BTC_USDT \
    PRESSURE_WORKERS="${PRESSURE_WORKERS:-2}" NOFILE_LIMIT=262144 \
    RUST_LOG=warning "$PRESSURE_BIN" >"$LOG_DIR/pressure-$i.log" 2>&1 &
  pids+=("$!")

  echo "pressure-$i: conns=$conns user_offset=$user_offset source_ips=$source_ip port=${PORTS[$i]}"
  user_offset=$((user_offset + conns))
done

wait "${pids[@]:$((DESK_COUNT + 1))}" || true

for ((i = 0; i < DESK_COUNT; i++)); do
  echo
  echo "===== pressure-$i -> ${PORTS[$i]} ====="
  sed -n '/final summary/,$p' "$LOG_DIR/pressure-$i.log"
done
