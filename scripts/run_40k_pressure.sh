#!/usr/bin/env bash
# 40K WebSocket connection pressure test.
# Works on macOS (lo0 aliases, psql, /tmp/aeron) and Linux (lo aliases,
# docker-postgres, /dev/shm/aeron, taskset aeronmd).
set -euo pipefail

# ── Platform ──────────────────────────────────────────────────────────────
IS_LINUX=false
[[ "$(uname -s)" == "Linux" ]] && IS_LINUX=true

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-$HOME/.cargo/global-target/release}"
DATABASE_URL="${DATABASE_URL:-postgres://user:password@localhost:5432/mydb}"
if $IS_LINUX; then
  AERON_DIR="${AERON_DIR:-/dev/shm/aeron}"
  AERON_BIN="${AERON_BIN:-$HOME/work/3party/aeron/cppbuild/Release/binaries/aeronmd}"
  # Isolated CPUs (isolcpus=0-11 at boot): aeronmd=0, engine=2, desk spin=4,6,8,10
  CPU_AERONMD="${CPU_AERONMD:-0}"
  CPU_ENGINE="${CPU_ENGINE:-2}"
  CPU_DESK_SPIN=(4 6 8 10 12 14 16 18)  # one per desk, physical isolated cores
  CPU_OTHERS="${CPU_OTHERS:-20-31}"      # tokio workers + gateway + pressure clients
else
  AERON_DIR="${AERON_DIR:-/tmp/aeron}"
fi
TOKENS_CSV="${TOKENS_CSV:-/tmp/pressure_users_40k.csv}"
TOTAL_CONNS="${TOTAL_CONNS:-40000}"
DESK_COUNT="${DESK_COUNT:-4}"
TRACER_ENABLED="${TRACER_ENABLED:-0}"
CONNS_PER_SOURCE_IP="${CONNS_PER_SOURCE_IP:-15000}"
PRESSURE_OWNER_SHARD_SHIFT="${PRESSURE_OWNER_SHARD_SHIFT:-0}"
COUNTER_FORWARD_DEBUG="${COUNTER_FORWARD_DEBUG:-0}"
ENGINE_RUST_LOG="${ENGINE_RUST_LOG:-warning}"
DESK_RUST_LOG="${DESK_RUST_LOG:-warning}"
MARKET_GATEWAY_RUST_LOG="${MARKET_GATEWAY_RUST_LOG:-warning}"
PRESSURE_RUST_LOG="${PRESSURE_RUST_LOG:-warning}"
LOG_DIR="${LOG_DIR:-/tmp/lightning-${TOTAL_CONNS}-${DESK_COUNT}desk-$(date +%Y%m%d-%H%M%S)}"

ENGINE_BIN="${ENGINE_BIN:-$BIN_DIR/exchange-engine}"
DESK_BIN="${DESK_BIN:-$BIN_DIR/desk-server}"
PRESSURE_BIN="${PRESSURE_BIN:-$BIN_DIR/pressure-client}"
GATEWAY_BIN="${GATEWAY_BIN:-$BIN_DIR/market-data-gateway}"

SOURCE_IPS=(127.0.0.2 127.0.0.3 127.0.0.4 127.0.0.5)
PORTS=(4003 4004 4005 4006)

# ── Validation ────────────────────────────────────────────────────────────
if (( DESK_COUNT < 1 || DESK_COUNT > ${#PORTS[@]} )); then
  echo "DESK_COUNT must be between 1 and ${#PORTS[@]} (got $DESK_COUNT)" >&2; exit 2
fi
[[ -f "$TOKENS_CSV" ]] || { echo "missing token csv: $TOKENS_CSV" >&2; exit 2; }

for ip in "${SOURCE_IPS[@]}"; do
  if $IS_LINUX; then
    ip addr show lo | grep -q "inet ${ip}/" \
      || { echo "missing lo alias ${ip}; run: sudo ip addr add ${ip}/8 dev lo" >&2; exit 2; }
  else
    ifconfig lo0 | grep -q "inet ${ip} " \
      || { echo "missing lo0 alias ${ip}; run: sudo ifconfig lo0 alias ${ip} up" >&2; exit 2; }
  fi
done

mkdir -p "$LOG_DIR"
echo "logs: $LOG_DIR"
echo "shape: ${TOTAL_CONNS} conns across ${DESK_COUNT} desk-server processes  TRACER_ENABLED=${TRACER_ENABLED} OWNER_SHARD_SHIFT=${PRESSURE_OWNER_SHARD_SHIFT} COUNTER_FORWARD_DEBUG=${COUNTER_FORWARD_DEBUG}"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

# ── Reset stale state ─────────────────────────────────────────────────────
pkill -f "exchange-engine|desk-server|market-data-gateway|redis-writer|pg-writer|pressure-client" 2>/dev/null || true
sleep 1
if $IS_LINUX; then
  docker exec work-postgres-1 psql -U user mydb -c \
    "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE status IN ('PENDING','TRADING');"
  docker exec work-postgres-1 psql -U user mydb -c \
    "UPDATE accounts SET frozen=0, updated_at=NOW() WHERE frozen <> 0;"
else
  psql "$DATABASE_URL" -c "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE status IN ('PENDING','TRADING');"
  psql "$DATABASE_URL" -c "UPDATE accounts SET frozen=0, updated_at=NOW() WHERE frozen <> 0;"
fi

# ── aeronmd (Linux: manage it; macOS: assumed already running) ────────────
if $IS_LINUX; then
  pkill -f aeronmd 2>/dev/null || true
  sleep 1.5
  rm -rf "$AERON_DIR"; mkdir -p "$AERON_DIR"
  env AERON_DIR="$AERON_DIR" taskset -c "$CPU_AERONMD" "$AERON_BIN" >"$LOG_DIR/aeronmd.log" 2>&1 &
  pid=$!
  pids+=("$pid")
  for i in $(seq 1 20); do
    [[ -f "$AERON_DIR/cnc.dat" ]] && break; sleep 1.5
  done
  [[ -f "$AERON_DIR/cnc.dat" ]] || { echo "aeronmd failed to start" >&2; exit 1; }
  echo "aeronmd ready (pid=$pid)"
fi

# ── exchange-engine ───────────────────────────────────────────────────────
if $IS_LINUX; then
  taskset -c "$CPU_ENGINE" \
  env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
    RUST_LOG="$ENGINE_RUST_LOG" TRACER_ENABLED=0 ENGINE_IDLE_SPINS=0 \
    ORDER_UPDATE_STREAM_COUNT="$DESK_COUNT" \
    "$ENGINE_BIN" >"$LOG_DIR/engine.log" 2>&1 &
else
  env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
    RUST_LOG="$ENGINE_RUST_LOG" TRACER_ENABLED=0 ENGINE_IDLE_SPINS=0 \
    ORDER_UPDATE_STREAM_COUNT="$DESK_COUNT" \
    "$ENGINE_BIN" >"$LOG_DIR/engine.log" 2>&1 &
fi
pid=$!
pids+=("$pid")
echo "exchange-engine started (pid=$pid)"
if $IS_LINUX; then sleep 5; else sleep 2; fi

# ── market-data-gateway ───────────────────────────────────────────────────
if [[ -x "$GATEWAY_BIN" ]]; then
  if $IS_LINUX; then
    taskset -c "$CPU_OTHERS" \
    env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
      RUST_LOG="$MARKET_GATEWAY_RUST_LOG" \
      "$GATEWAY_BIN" >"$LOG_DIR/market-gateway.log" 2>&1 &
  else
    env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
      RUST_LOG="$MARKET_GATEWAY_RUST_LOG" \
      "$GATEWAY_BIN" >"$LOG_DIR/market-gateway.log" 2>&1 &
  fi
  pids+=("$!")
fi

# ── desk-servers ──────────────────────────────────────────────────────────
for ((i = 0; i < DESK_COUNT; i++)); do
  if $IS_LINUX; then
    spin_cpu="${CPU_DESK_SPIN[$i]}"
    taskset -c "${spin_cpu},${CPU_OTHERS}" \
    env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
      RUST_LOG="$DESK_RUST_LOG" TRACER_ENABLED="$TRACER_ENABLED" \
      COUNTER_FORWARD_DEBUG="$COUNTER_FORWARD_DEBUG" \
      DESK_SPIN="${DESK_SPIN:-true}" DESK_SEND_CORE="$spin_cpu" \
      DESK_PORT="${PORTS[$i]}" DESK_ID="$i" \
      TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-2}" NOFILE_LIMIT=262144 \
      "$DESK_BIN" >"$LOG_DIR/desk-$i.log" 2>&1 &
    echo "desk-$i  spin_cpu=$spin_cpu  port=${PORTS[$i]}"
  else
    env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
      RUST_LOG="$DESK_RUST_LOG" TRACER_ENABLED="$TRACER_ENABLED" \
      COUNTER_FORWARD_DEBUG="$COUNTER_FORWARD_DEBUG" \
      DESK_SPIN="${DESK_SPIN:-true}" \
      DESK_PORT="${PORTS[$i]}" DESK_ID="$i" \
      TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-2}" NOFILE_LIMIT=262144 \
      "$DESK_BIN" >"$LOG_DIR/desk-$i.log" 2>&1 &
  fi
  pids+=("$!")
  # Linux: stagger desk starts — pub pre-creation serializes registrations
  if $IS_LINUX; then sleep 1.5; fi
done
sleep "${DESK_WARMUP_S:-8}"

# ── pressure clients ──────────────────────────────────────────────────────
base_conns=$((TOTAL_CONNS / DESK_COUNT))
extra_conns=$((TOTAL_CONNS % DESK_COUNT))
ip_index=0

for ((i = 0; i < DESK_COUNT; i++)); do
  conns=$base_conns
  (( i < extra_conns )) && conns=$((conns + 1))

  ips_needed=$(((conns + CONNS_PER_SOURCE_IP - 1) / CONNS_PER_SOURCE_IP))
  if (( ip_index + ips_needed > ${#SOURCE_IPS[@]} )); then
    echo "not enough source IPs for ${TOTAL_CONNS}/${DESK_COUNT}; need ${ips_needed} more for desk $i" >&2; exit 2
  fi
  source_ip="${SOURCE_IPS[$ip_index]}"
  for ((j = 1; j < ips_needed; j++)); do
    source_ip="${source_ip},${SOURCE_IPS[$((ip_index + j))]}"
  done
  ip_index=$((ip_index + ips_needed))

  owner_shard=$(((i + PRESSURE_OWNER_SHARD_SHIFT) % DESK_COUNT))

  env PRESSURE_TOKENS_CSV="$TOKENS_CSV" PRESSURE_USERS="$conns" \
    PRESSURE_USER_OFFSET=0 PRESSURE_OWNER_SHARD="$owner_shard" PRESSURE_OWNER_SHARD_COUNT="$DESK_COUNT" \
    PRESSURE_CONNS="$conns" \
    PRESSURE_DURATION_S="${PRESSURE_DURATION_S:-60}" \
    PRESSURE_RAMP_S="${PRESSURE_RAMP_S:-30}" \
    PRESSURE_OPS_PER_SEC="${PRESSURE_OPS_PER_SEC:-0.2}" \
    PRESSURE_BASE_URL="http://127.0.0.1:${PORTS[$i]}" \
    PRESSURE_SOURCE_IPS="$source_ip" PRESSURE_SYMBOL=BTC_USDT \
    PRESSURE_WORKERS="${PRESSURE_WORKERS:-2}" NOFILE_LIMIT=262144 \
    RUST_LOG="$PRESSURE_RUST_LOG" "$PRESSURE_BIN" >"$LOG_DIR/pressure-$i.log" 2>&1 &
  pids+=("$!")
  echo "pressure-$i: conns=$conns owner_shard=$owner_shard source_ips=$source_ip port=${PORTS[$i]}"
done

# Wait for pressure clients only (skip long-running services)
if [[ -x "$GATEWAY_BIN" ]]; then
  _pids_skip=$((DESK_COUNT + 2))  # engine + gateway + desks
else
  _pids_skip=$((DESK_COUNT + 1))  # engine + desks
fi
if $IS_LINUX; then _pids_skip=$((_pids_skip + 1)); fi  # +1 for aeronmd on Linux
wait "${pids[@]:$_pids_skip}" || true

echo
echo "===== ${TOTAL_CONNS} CLIENT-SIDE LATENCY (${DESK_COUNT} desk) ====="
for ((i = 0; i < DESK_COUNT; i++)); do
  echo
  echo "----- pressure-$i -> ${PORTS[$i]} -----"
  sed -n '/final summary/,$p' "$LOG_DIR/pressure-$i.log"
done
