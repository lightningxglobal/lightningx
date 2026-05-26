#!/bin/bash
set -e

# 确保 aeronmd 已启动
export AERON_DIR=${AERON_DIR:-/tmp/aeron}
mkdir -p "$AERON_DIR"

echo "═══════════════════════════════════════════════════════════════"
echo "              Aeron Message Flow Test"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "AERON_DIR: $AERON_DIR"
echo ""

# 启动服务器（后台）
echo "[1/2] 启动 Demo 服务器..."
timeout 10 cargo run --example aeron_integration_demo 2>&1 | grep -E "(✓|✅|OrderInbound|AeronOrder|Inbound order|received message|mutex)" &
SERVER_PID=$!

# 等待服务器启动
sleep 2

# 启动客户端
echo "[2/2] 启动交易客户端..."
echo ""
timeout 8 cargo run --release --example trading_client 2>&1 | grep -v "^   " | head -100 &
CLIENT_PID=$!

# 等待两个进程完成
wait $SERVER_PID 2>/dev/null || true
wait $CLIENT_PID 2>/dev/null || true

echo ""
echo "═══════════════════════════════════════════════════════════════"
