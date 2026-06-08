# Lightning Exchange — Swap API 指南 (testnet)

完整机器可读规范见 [`openapi.yaml`](./openapi.yaml)。下面是上手要点。

## 认证
```bash
# 注册（testnet 自动发 10,000 USDT 测试资金）
curl -s localhost:4003/api/auth/register \
  -d '{"email":"you@test","password":"pw_at_least_8"}'
# 登录拿 JWT
TOKEN=$(curl -s localhost:4003/api/auth/login \
  -d '{"email":"you@test","password":"pw_at_least_8"}' | jq -r .token)
```
之后所有私有端点带 `Authorization: Bearer $TOKEN`。

## 下单 / 撤单
```bash
# 限价开多 0.01 BTC @ $50,000
curl -s localhost:4003/api/orders -H "Authorization: Bearer $TOKEN" \
  -d '{"symbol":"BTC_USDT","side":"buy","order_type":"limit","price":50000,"quantity":0.01}'
# 市价单（不带 price）
curl -s localhost:4003/api/orders -H "Authorization: Bearer $TOKEN" \
  -d '{"symbol":"BTC_USDT","side":"sell","order_type":"market","quantity":0.01}'
# 只减仓（不会加仓/翻仓）
curl -s localhost:4003/api/orders -H "Authorization: Bearer $TOKEN" \
  -d '{"symbol":"BTC_USDT","side":"sell","order_type":"market","quantity":0.01,"reduce_only":true}'
# 撤单
curl -s -X DELETE localhost:4003/api/orders/12345 -H "Authorization: Bearer $TOKEN"
```

## 止损 / 止盈（触发单）
```bash
# 多头止损：标记价跌破 $48,000 时市价平 0.01
curl -s localhost:4003/api/trigger-orders -H "Authorization: Bearer $TOKEN" \
  -d '{"symbol":"BTC_USDT","side":"sell","order_type":"market",
       "trigger_price":48000,"trigger_when":"falling","quantity":0.01}'
```
触发判定基于**标记价**（外部指数钳制后的价，非本所盘口），防操纵。
不带 `trigger_when` 时默认按该方向的止损语义（buy=rising, sell=falling）。

## 查询
- `GET /api/positions` — 实时持仓（内存，含未实现盈亏）
- `GET /api/accounts` — 余额（`*_str` 字段是精确小数）
- `GET /api/funding?symbol=BTC_USDT` — 当前/下次资金费、溢价估计
- `GET /api/trades` `GET /api/tickers` `GET /api/klines`

## WebSocket (`/ws`)
JSON 帧，`type` 字段路由。客户端 → 服务端：
```json
{"type":"subscribe","channels":["trades.BTC_USDT","depth.BTC_USDT"]}
{"type":"place_order","symbol":"BTC_USDT","side":"buy","order_type":"limit","price":50000,"quantity":0.01}
{"type":"place_orders","batch_id":1,"orders":[ ... ]}
{"type":"cancel_order","order_id":12345}
{"type":"batch_cancel","order_ids":[1,2,3]}
{"type":"unsubscribe","channels":["depth.BTC_USDT"]}
```
服务端 → 客户端：成交/订单状态推送（个人频道，登录后自动）、行情广播
（订阅后）。私有订单更新延迟 <1ms（与公共行情分线程）。

## 运营（admin，需 EXCHANGE_ADMIN_TOKEN）
```bash
# 停牌 / 复牌 + 改费率（即时生效，免重启）
curl -s localhost:4003/api/admin/config -H "Authorization: Bearer $ADMIN" \
  -d '{"symbol":"BTC_USDT","trading_halted":true}'
curl -s localhost:4003/api/admin/config -H "Authorization: Bearer $ADMIN" \
  -d '{"symbol":"BTC_USDT","trading_halted":false,"taker_fee_bps":5,"maker_fee_bps":1}'
# 人工强平某用户某品种持仓
curl -s localhost:4003/api/admin/force-close -H "Authorization: Bearer $ADMIN" \
  -d '{"user_id":42,"symbol":"BTC_USDT"}'
```

## 沙箱
testnet 本身即沙箱：资金来自注册种子 + `/api/test-funds` + admin 调账，
无真实资金风险。
