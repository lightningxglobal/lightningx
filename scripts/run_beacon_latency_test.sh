#!/usr/bin/env bash
# Internal end-to-end latency test using beacon → VictoriaMetrics.
# Measures the gap between MS_WS_ORDER_RECV (8<<16|27) and
# MS_WS_RESPONSE_SENT (8<<16|37) — server-side processing only.
#
# Works on macOS (M4) and Ubuntu Linux.
#
# Prerequisites:
#   - beacon binary: ~/.cargo/global-target/release/beacon
#   - VictoriaMetrics running on localhost:8428 (Docker on Ubuntu, or local on Mac)
#   - Pressure-test tokens: $TOKENS_CSV (default /tmp/pressure_users_40k.csv)
#   - loopback aliases: 127.0.0.2 ~ 127.0.0.5
#
# Usage:
#   bash scripts/run_beacon_latency_test.sh
#   TOTAL_CONNS=100000 DESK_COUNT=4 bash scripts/run_beacon_latency_test.sh
set -euo pipefail

IS_LINUX=false
[[ "$(uname -s)" == "Linux" ]] && IS_LINUX=true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PRESSURE_SCRIPT can be set explicitly; otherwise auto-select by TOTAL_CONNS/DESK_COUNT.
if [[ -z "${PRESSURE_SCRIPT:-}" ]]; then
  _conns="${TOTAL_CONNS:-40000}"
  _desks="${DESK_COUNT:-4}"
  if (( _desks >= 8 )); then
    _base="run_200k_pressure.sh"
  elif (( _conns > 40000 )); then
    _base="run_100k_pressure.sh"
  else
    _base="run_40k_pressure.sh"
  fi
  if [[ -f "$SCRIPT_DIR/$_base" ]]; then
    PRESSURE_SCRIPT="$SCRIPT_DIR/$_base"
  else
    PRESSURE_SCRIPT="/tmp/$_base"
  fi
fi
BIN_DIR="${BIN_DIR:-$HOME/.cargo/global-target/release}"
if $IS_LINUX; then
  AERON_DIR="${AERON_DIR:-/dev/shm/aeron}"
else
  AERON_DIR="${AERON_DIR:-/tmp/aeron}"
fi
VM_ENDPOINT="${VM_ENDPOINT:-http://localhost:8428}"

BEACON_BIN="${BEACON_BIN:-$BIN_DIR/beacon}"
BEACON_CONFIG="/tmp/beacon_latency.yaml"

[[ -x "$BEACON_BIN" ]] || { echo "beacon binary not found: $BEACON_BIN" >&2; exit 2; }

# ── Write beacon config ───────────────────────────────────────────────────────
cat > "$BEACON_CONFIG" <<EOF
aeron:
  media_driver_dir: "$AERON_DIR"
  channel: "aeron:ipc"
  stream_id: 1001
  poll_interval_ms: 1
  receiver_queue_capacity: 1048576

aggregation:
  flush_interval_secs: 10
  outlier_threshold: 1.5
  warmup_samples: 1000
  trace_ttl_secs: 3
  max_traces: 100000

publisher:
  endpoint: "$VM_ENDPOINT"
  logging_downstream: true
  circuit_breaker:
    failure_threshold: 5
    timeout_secs: 30

registry:
  scenario_profiles: []
  groups:
    everything: true

http:
  bind_address: "0.0.0.0:8082"

logging:
  level: "info"
  format: "json"
EOF

echo "beacon config written to $BEACON_CONFIG (VictoriaMetrics: $VM_ENDPOINT)"

# ── Kill any stale beacon ─────────────────────────────────────────────────────
pkill -f "$BEACON_BIN" 2>/dev/null || true
sleep 1

# ── Start beacon ─────────────────────────────────────────────────────────────
LOG_DIR="/tmp/beacon-latency-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$LOG_DIR"

"$BEACON_BIN" run --config "$BEACON_CONFIG" > "$LOG_DIR/beacon.log" 2>&1 &
BEACON_PID=$!
echo "beacon started (pid=$BEACON_PID), logs: $LOG_DIR/beacon.log"
sleep 2

if ! kill -0 "$BEACON_PID" 2>/dev/null; then
  echo "beacon failed to start — check $LOG_DIR/beacon.log" >&2
  cat "$LOG_DIR/beacon.log" >&2
  exit 1
fi

# ── Run 40K pressure test with TRACER_ENABLED=1 ───────────────────────────────
echo "starting 40K pressure test with TRACER_ENABLED=1 ..."
TRACER_ENABLED=1 \
DESK_SPIN="${DESK_SPIN:-true}" \
TOTAL_CONNS="${TOTAL_CONNS:-40000}" \
DESK_COUNT="${DESK_COUNT:-4}" \
PRESSURE_DURATION_S="${PRESSURE_DURATION_S:-120}" \
PRESSURE_RAMP_S="${PRESSURE_RAMP_S:-30}" \
bash "$PRESSURE_SCRIPT"

# ── Shutdown beacon ───────────────────────────────────────────────────────────
echo "test complete — waiting 15s for beacon to flush final histograms to VictoriaMetrics..."
sleep 15
kill "$BEACON_PID" 2>/dev/null || true

echo
echo "===== BEACON LOG (last 30 lines) ====="
tail -30 "$LOG_DIR/beacon.log"
echo
echo "VictoriaMetrics query (last 10 min):"
echo "  http://localhost:8428/api/v1/query?query=latency_us"
echo "  Grafana: http://localhost:3000"
