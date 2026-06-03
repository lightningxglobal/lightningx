#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-/Users/alphawu/.cargo/global-target/release}"
DATABASE_URL="${DATABASE_URL:-postgres://user:password@localhost:5432/mydb}"
AERON_DIR="${AERON_DIR:-/tmp/aeron}"
TOKENS_CSV="${TOKENS_CSV:-/tmp/pressure_users_40k.csv}"
LOG_DIR="${LOG_DIR:-/tmp/lightning-40k-$(date +%Y%m%d-%H%M%S)}"

ENGINE_BIN="${ENGINE_BIN:-$BIN_DIR/exchange-engine}"
DESK_BIN="${DESK_BIN:-$BIN_DIR/desk-server}"
PRESSURE_BIN="${PRESSURE_BIN:-$BIN_DIR/pressure-client}"

SOURCE_IPS=(127.0.0.2 127.0.0.3 127.0.0.4 127.0.0.5)
PORTS=(4003 4004 4005 4006)

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

for i in "${!PORTS[@]}"; do
  env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
    RUST_LOG=warning TRACER_ENABLED=0 DESK_PORT="${PORTS[$i]}" DESK_ID="$i" \
    TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-2}" NOFILE_LIMIT=262144 \
    "$DESK_BIN" >"$LOG_DIR/desk-$i.log" 2>&1 &
  pids+=("$!")
done
sleep 5

for i in "${!PORTS[@]}"; do
  env PRESSURE_TOKENS_CSV="$TOKENS_CSV" PRESSURE_USERS=10000 \
    PRESSURE_USER_OFFSET="$((i * 10000))" PRESSURE_CONNS=10000 \
    PRESSURE_DURATION_S="${PRESSURE_DURATION_S:-30}" \
    PRESSURE_RAMP_S="${PRESSURE_RAMP_S:-30}" \
    PRESSURE_OPS_PER_SEC="${PRESSURE_OPS_PER_SEC:-0.2}" \
    PRESSURE_BASE_URL="http://127.0.0.1:${PORTS[$i]}" \
    PRESSURE_SOURCE_IPS="${SOURCE_IPS[$i]}" PRESSURE_SYMBOL=BTC_USDT \
    PRESSURE_WORKERS="${PRESSURE_WORKERS:-2}" NOFILE_LIMIT=262144 \
    RUST_LOG=warning "$PRESSURE_BIN" >"$LOG_DIR/pressure-$i.log" 2>&1 &
  pids+=("$!")
done

wait "${pids[@]:5}" || true

for i in "${!PORTS[@]}"; do
  echo
  echo "===== pressure-$i (${SOURCE_IPS[$i]} -> ${PORTS[$i]}) ====="
  sed -n '/final summary/,$p' "$LOG_DIR/pressure-$i.log"
done
