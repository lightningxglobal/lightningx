#!/usr/bin/env bash
set -e
SESSION="lightning"

# 检查 cargo-watch
if ! command -v cargo-watch &>/dev/null; then
  echo "Installing cargo-watch..."
  cargo install cargo-watch
fi

# 如果 session 已存在，直接 attach
if tmux has-session -t "$SESSION" 2>/dev/null; then
  exec tmux attach -t "$SESSION"
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

tmux new-session  -d -s "$SESSION" -n matching \
  "cd '$SCRIPT_DIR' && DATABASE_URL=postgres://user:password@localhost:5432/mydb JWT_SECRET=change_me_in_production PORT=4001 cargo watch -q -w src -x 'run --bin exchange-server'; read"
tmux new-window   -t "$SESSION" -n desk \
  "cd '$SCRIPT_DIR' && DATABASE_URL=postgres://user:password@localhost:5432/mydb DESK_PORT=4003 UPSTREAM_WS_URL=ws://127.0.0.1:4001/ws cargo watch -q -w src -x 'run --bin desk-server'; read"
tmux new-window   -t "$SESSION" -n data \
  "cd '$SCRIPT_DIR' && DATABASE_URL=postgres://user:password@localhost:5432/mydb DATA_PORT=4002 cargo watch -q -w src -x 'run --bin lightning-data'; read"
tmux new-window   -t "$SESSION" -n frontend \
  "cd '$SCRIPT_DIR/../exchange-frontend' && npm run dev; read"

tmux select-window -t "$SESSION:matching"
exec tmux attach   -t "$SESSION"
