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

# 撮合引擎 (Aeron-based, communicates with desk-server via Aeron IPC)
tmux new-session  -d -s "$SESSION" -n engine \
  "cd '$SCRIPT_DIR' && DATABASE_URL=postgres://user:password@localhost:5432/mydb cargo watch -q -w src -x 'run --bin exchange-engine'; read"

# 柜台服务 (REST + WebSocket API, communicates with engine via Aeron)
tmux new-window   -t "$SESSION" -n desk \
  "cd '$SCRIPT_DIR' && DATABASE_URL=postgres://user:password@localhost:5432/mydb DESK_PORT=4003 cargo watch -q -w src -x 'run --bin desk-server'; read"

# 做市商 (mirrors Binance prices, keeps orderbook liquid)
tmux new-window   -t "$SESSION" -n maker \
  "cd '$SCRIPT_DIR' && EXCHANGE_URL=http://localhost:4003 cargo watch -q -w src -x 'run --bin market-maker'; read"

# 交易机器人 (generates synthetic trading activity)
tmux new-window   -t "$SESSION" -n bot \
  "cd '$SCRIPT_DIR' && EXCHANGE_URL=http://localhost:4003 cargo watch -q -w src -x 'run --bin trade-bot'; read"

# 前端
tmux new-window   -t "$SESSION" -n frontend \
  "cd '$SCRIPT_DIR/../../exchange-frontend' && npm run dev; read"

tmux select-window -t "$SESSION:engine"
exec tmux attach   -t "$SESSION"
