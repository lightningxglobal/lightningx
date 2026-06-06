#!/usr/bin/env bash
# 400K WebSocket connection pressure test.
# 8 desk-servers × 50K connections each.
# Works on macOS (lo0 aliases, psql, /tmp/aeron) and Linux (lo aliases,
# docker-postgres, /dev/shm/aeron, taskset aeronmd).
#
# Required loopback aliases for the default 32 source IP pool:
#   macOS: for i in $(seq 2 33); do sudo ifconfig lo0 alias 127.0.0.$i up; done
#   Linux: for i in $(seq 2 33); do sudo ip addr add 127.0.0.$i/8 dev lo; done
#
# No-forward: same counter_shard layout as 200K (8 desks, COUNTER_SHARD_COUNT=4).
# owner_shard = desk_id % 4, user_offset = (desk_id / 4) * 50K.
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
  CPU_AERONMD="${CPU_AERONMD:-0}"
  CPU_ENGINE="${CPU_ENGINE:-2}"
  CPU_DESK_SPIN=(4 8 4 8 4 8 4 8)  # only CPU4/CPU8 (adjacent to engine on CPU2)
  CPU_OTHERS="${CPU_OTHERS:-20-31}"
else
  AERON_DIR="${AERON_DIR:-/tmp/aeron}"
fi
TOKENS_CSV="${TOKENS_CSV:-/tmp/pressure_users_400k.csv}"
TOTAL_CONNS="${TOTAL_CONNS:-400000}"
DESK_COUNT="${DESK_COUNT:-8}"
TRACER_ENABLED="${TRACER_ENABLED:-0}"
DESK_SPIN="${DESK_SPIN:-true}"
CONNS_PER_SOURCE_IP="${CONNS_PER_SOURCE_IP:-15000}"
LOG_DIR="${LOG_DIR:-/tmp/lightning-${TOTAL_CONNS}-${DESK_COUNT}desk-$(date +%Y%m%d-%H%M%S)}"

ENGINE_BIN="${ENGINE_BIN:-$BIN_DIR/exchange-engine}"
DESK_BIN="${DESK_BIN:-$BIN_DIR/desk-server}"
PRESSURE_BIN="${PRESSURE_BIN:-$BIN_DIR/pressure-client}"
GATEWAY_BIN="${GATEWAY_BIN:-$BIN_DIR/market-data-gateway}"

SOURCE_IP_POOL=(
  127.0.0.2  127.0.0.3  127.0.0.4  127.0.0.5
  127.0.0.6  127.0.0.7  127.0.0.8  127.0.0.9
  127.0.0.10 127.0.0.11 127.0.0.12 127.0.0.13
  127.0.0.14 127.0.0.15 127.0.0.16 127.0.0.17
  127.0.0.18 127.0.0.19 127.0.0.20 127.0.0.21
  127.0.0.22 127.0.0.23 127.0.0.24 127.0.0.25
  127.0.0.26 127.0.0.27 127.0.0.28 127.0.0.29
  127.0.0.30 127.0.0.31 127.0.0.32 127.0.0.33
)
PORTS=(4003 4004 4005 4006 4007 4008 4009 4010)

# ── Validation ────────────────────────────────────────────────────────────
if (( DESK_COUNT < 1 || DESK_COUNT > ${#PORTS[@]} )); then
  echo "DESK_COUNT must be 1-${#PORTS[@]} (got $DESK_COUNT)" >&2; exit 2
fi
[[ -f "$TOKENS_CSV" ]] || { echo "missing token csv: $TOKENS_CSV" >&2; exit 2; }
token_rows=$(wc -l < "$TOKENS_CSV" | tr -d ' ')
if (( token_rows < TOTAL_CONNS )); then
  echo "token csv has $token_rows rows, need $TOTAL_CONNS" >&2; exit 2
fi

conns_per_desk=$(( (TOTAL_CONNS + DESK_COUNT - 1) / DESK_COUNT ))
ips_per_desk=$(( (conns_per_desk + CONNS_PER_SOURCE_IP - 1) / CONNS_PER_SOURCE_IP ))
total_ips_needed=$(( ips_per_desk * DESK_COUNT ))
for ip in "${SOURCE_IP_POOL[@]::$total_ips_needed}"; do
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
echo "shape: ${TOTAL_CONNS} conns across ${DESK_COUNT} desk-server processes  TRACER_ENABLED=${TRACER_ENABLED}"

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

# ── aeronmd ───────────────────────────────────────────────────────────────
REUSE_AERONMD="${REUSE_AERONMD:-0}"
if $IS_LINUX; then
  if [[ "$REUSE_AERONMD" == "1" ]] && pgrep -f aeronmd > /dev/null && [[ -f "$AERON_DIR/cnc.dat" ]]; then
    echo "aeronmd already running, reusing (pid=$(pgrep -f aeronmd | head -1))"
  else
    pkill -f aeronmd 2>/dev/null || true
    sleep 1.5
    rm -rf "$AERON_DIR"; mkdir -p "$AERON_DIR"
    env AERON_DIR="$AERON_DIR" taskset -c "$CPU_AERONMD" "$AERON_BIN" >"$LOG_DIR/aeronmd.log" 2>&1 &
    pids+=("$!")
    for i in $(seq 1 20); do
      [[ -f "$AERON_DIR/cnc.dat" ]] && break; sleep 1.5
    done
    [[ -f "$AERON_DIR/cnc.dat" ]] || { echo "aeronmd failed to start" >&2; exit 1; }
    echo "aeronmd ready (pid=${pids[-1]})"
  fi
fi

# ── exchange-engine ───────────────────────────────────────────────────────
if $IS_LINUX; then
  taskset -c "$CPU_ENGINE" \
  env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
    RUST_LOG=warning TRACER_ENABLED=0 ENGINE_IDLE_SPINS=0 \
    ORDER_UPDATE_STREAM_COUNT="$DESK_COUNT" \
    "$ENGINE_BIN" >"$LOG_DIR/engine.log" 2>&1 &
else
  env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
    RUST_LOG=warning TRACER_ENABLED=0 ENGINE_IDLE_SPINS=0 \
    ORDER_UPDATE_STREAM_COUNT="$DESK_COUNT" \
    "$ENGINE_BIN" >"$LOG_DIR/engine.log" 2>&1 &
fi
pids+=("$!")
echo "exchange-engine started (pid=${pids[-1]})"
if $IS_LINUX; then sleep 5; else sleep 2; fi

# ── market-data-gateway ───────────────────────────────────────────────────
if [[ -x "$GATEWAY_BIN" ]]; then
  if $IS_LINUX; then
    taskset -c "$CPU_OTHERS" \
    env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
      RUST_LOG=warning \
      "$GATEWAY_BIN" >"$LOG_DIR/market-gateway.log" 2>&1 &
  else
    env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
      RUST_LOG=warning \
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
      RUST_LOG=warning TRACER_ENABLED="$TRACER_ENABLED" \
      DESK_SPIN="$DESK_SPIN" DESK_SEND_CORE="$spin_cpu" \
      DESK_PORT="${PORTS[$i]}" DESK_ID="$i" \
      TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-4}" NOFILE_LIMIT=524288 \
      "$DESK_BIN" >"$LOG_DIR/desk-$i.log" 2>&1 &
    echo "desk-$i  spin_cpu=$spin_cpu  port=${PORTS[$i]}"
  else
    env DATABASE_URL="$DATABASE_URL" AERON_DIR="$AERON_DIR" SYMBOLS=BTC_USDT \
      RUST_LOG=warning TRACER_ENABLED="$TRACER_ENABLED" \
      DESK_SPIN="$DESK_SPIN" \
      DESK_PORT="${PORTS[$i]}" DESK_ID="$i" \
      TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-4}" NOFILE_LIMIT=524288 \
      "$DESK_BIN" >"$LOG_DIR/desk-$i.log" 2>&1 &
  fi
  pids+=("$!")
  if $IS_LINUX; then sleep 1.5; fi
done
sleep "${DESK_WARMUP_S:-12}"

# ── pressure clients ──────────────────────────────────────────────────────
# 8 desks, COUNTER_SHARD_COUNT=4: desk i shares counter_shard i%4 with desk i+4.
# owner_shard = i%4, user_offset = (i/4)*conns_per_desk → no forwarding.
base_conns=$((TOTAL_CONNS / DESK_COUNT))
extra_conns=$((TOTAL_CONNS % DESK_COUNT))
ip_index=0

for ((i = 0; i < DESK_COUNT; i++)); do
  conns=$base_conns
  (( i < extra_conns )) && conns=$((conns + 1))

  ips_needed=$(((conns + CONNS_PER_SOURCE_IP - 1) / CONNS_PER_SOURCE_IP))
  if (( ip_index + ips_needed > ${#SOURCE_IP_POOL[@]} )); then
    echo "not enough source IPs for desk $i (need $ips_needed more)" >&2; exit 2
  fi
  source_ip="${SOURCE_IP_POOL[$ip_index]}"
  for ((j = 1; j < ips_needed; j++)); do
    source_ip="${source_ip},${SOURCE_IP_POOL[$((ip_index + j))]}"
  done
  ip_index=$((ip_index + ips_needed))

  owner_shard=$(( i % 4 ))
  layer=$(( i / 4 ))
  user_offset=$(( layer * base_conns ))

  env PRESSURE_TOKENS_CSV="$TOKENS_CSV" \
    PRESSURE_USERS="$conns" PRESSURE_USER_OFFSET="$user_offset" \
    PRESSURE_OWNER_SHARD="$owner_shard" PRESSURE_OWNER_SHARD_COUNT=4 \
    PRESSURE_CONNS="$conns" \
    PRESSURE_DURATION_S="${PRESSURE_DURATION_S:-60}" \
    PRESSURE_RAMP_S="${PRESSURE_RAMP_S:-90}" \
    PRESSURE_OPS_PER_SEC="${PRESSURE_OPS_PER_SEC:-0.2}" \
    PRESSURE_BASE_URL="http://127.0.0.1:${PORTS[$i]}" \
    PRESSURE_SOURCE_IPS="$source_ip" PRESSURE_SYMBOL=BTC_USDT \
    PRESSURE_WORKERS="${PRESSURE_WORKERS:-4}" NOFILE_LIMIT=524288 \
    RUST_LOG=warning "$PRESSURE_BIN" >"$LOG_DIR/pressure-$i.log" 2>&1 &
  pids+=("$!")
  echo "pressure-$i: conns=$conns owner_shard=$owner_shard user_offset=$user_offset source_ips=$source_ip port=${PORTS[$i]}"
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
