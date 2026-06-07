#!/usr/bin/env bash
# Generate pressure test token CSVs from PostgreSQL.
# Reads existing pressure_{idx}@stress.test users and writes user_id,idx CSVs.
# Output dir persists across reboots (~/work/pressure_tokens/ not /tmp/).
#
# Usage:
#   bash scripts/gen_pressure_tokens.sh              # 40K + 100K + 200K
#   TOTAL_CONNS=400000 bash scripts/gen_pressure_tokens.sh
set -euo pipefail

IS_LINUX=false
[[ "$(uname -s)" == "Linux" ]] && IS_LINUX=true

DATABASE_URL="${DATABASE_URL:-postgres://user:password@localhost:5432/mydb}"
OUT_DIR="${OUT_DIR:-$HOME/work/pressure_tokens}"
TOTAL_CONNS="${TOTAL_CONNS:-200000}"

mkdir -p "$OUT_DIR"

SQL="SELECT id, (regexp_replace(email, 'pressure_(\d+)@stress\.test', '\\1'))::integer AS idx
     FROM users
     WHERE email ~ '^pressure_\d+@stress\.test\$'
     ORDER BY 2
     LIMIT $TOTAL_CONNS"

OUT="$OUT_DIR/pressure_users_${TOTAL_CONNS}.csv"

echo "querying $TOTAL_CONNS users from DB → $OUT"
if $IS_LINUX; then
  docker exec work-postgres-1 psql -U user mydb -t -A -F"," -c "$SQL" > "$OUT"
else
  psql "$DATABASE_URL" -t -A -F"," -c "$SQL" > "$OUT"
fi

got=$(wc -l < "$OUT" | tr -d ' ')
echo "wrote $got rows"
if (( got < TOTAL_CONNS )); then
  echo "WARNING: only $got users in DB, need $TOTAL_CONNS — run pressure-client with PRESSURE_SETUP_ONLY=1 first" >&2
fi

# Derive smaller CSVs from the full one.
for n in 40000 100000; do
  if (( n < TOTAL_CONNS )); then
    sub="$OUT_DIR/pressure_users_${n}.csv"
    head -n "$n" "$OUT" > "$sub"
    echo "derived $sub ($n rows)"
  fi
done
