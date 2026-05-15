#!/bin/bash

# 完整交易系统演示脚本
# 启动Aeron Media Driver、撮合系统和交易客户端

set -e

# 配置
AERON_DIR=${AERON_DIR:-/tmp/aeron}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "════════════════════════════════════════════════════════════════════"
echo "         完整交易系统演示"
echo "════════════════════════════════════════════════════════════════════"
echo ""
echo "目录结构:"
echo "  Project Dir: $PROJECT_DIR"
echo "  Aeron Dir:   $AERON_DIR"
echo ""

# 检查Aeron Media Driver是否运行
echo "检查Aeron Media Driver..."
if ! pgrep -f "aeronmd" > /dev/null; then
    echo "⚠ 警告: Aeron Media Driver未运行"
    echo "请在另一个终端运行: AERON_DIR=$AERON_DIR aeronmd"
    echo ""
fi

# 启动撮合系统
echo "════════════════════════════════════════════════════════════════════"
echo "1️⃣  启动撮合系统 (Matching Engine + Aeron Transport)"
echo "════════════════════════════════════════════════════════════════════"
echo ""

AERON_DIR=$AERON_DIR cargo run --release --example aeron_integration_demo &
MATCHING_PID=$!

# 等待撮合系统启动
sleep 3

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "2️⃣  启动交易客户端"
echo "════════════════════════════════════════════════════════════════════"
echo ""

AERON_DIR=$AERON_DIR cargo run --release --example trading_client &
CLIENT_PID=$!

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "系统运行中..."
echo "════════════════════════════════════════════════════════════════════"
echo ""
echo "进程:"
echo "  撮合系统 PID: $MATCHING_PID"
echo "  客户端 PID:   $CLIENT_PID"
echo ""
echo "按 Ctrl+C 停止演示"
echo ""

# 等待任一进程完成
wait -n $MATCHING_PID $CLIENT_PID 2>/dev/null || true

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "演示完成"
echo "════════════════════════════════════════════════════════════════════"

# 清理
kill $MATCHING_PID 2>/dev/null || true
kill $CLIENT_PID 2>/dev/null || true
wait 2>/dev/null || true
