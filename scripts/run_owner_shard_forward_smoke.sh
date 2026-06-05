#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export DESK_COUNT="${DESK_COUNT:-4}"
export TOTAL_CONNS="${TOTAL_CONNS:-4}"
export PRESSURE_OWNER_SHARD_SHIFT="${PRESSURE_OWNER_SHARD_SHIFT:-1}"
export PRESSURE_DURATION_S="${PRESSURE_DURATION_S:-8}"
export PRESSURE_RAMP_S="${PRESSURE_RAMP_S:-1}"
export PRESSURE_OPS_PER_SEC="${PRESSURE_OPS_PER_SEC:-2}"
export TRACER_ENABLED="${TRACER_ENABLED:-0}"
export LOG_DIR="${LOG_DIR:-/tmp/lightning-owner-forward-smoke-$(date +%Y%m%d-%H%M%S)}"

"$ROOT/scripts/run_40k_pressure.sh"
