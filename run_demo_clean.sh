#!/bin/bash

echo "════════════════════════════════════════════════════════════════"
echo "          Aeron Integration Demo - Clean Logs"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "启动服务器 (Stream 1 接收订单)..."
echo ""

cargo run --example aeron_integration_demo 2>&1 | grep -v "poll #" | grep -v "acquiring" | grep -v "calling" | grep -v "returned:"
