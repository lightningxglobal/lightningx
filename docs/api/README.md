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

## 充值入账接口（链上服务调用，service token）
链上充值监听器在确认 N 个区块后,对每笔到账**调用一次**(按
`(chain, tx_hash, log_index)` 幂等,重放/重组重扫不会重复入账)。撮合
引擎完全不参与——入账是账本上的一笔 AccountSet + fund_audit,单事务。
```bash
curl -s localhost:4003/api/deposit/credit   -H "Authorization: Bearer $EXCHANGE_DEPOSIT_TOKEN"   -d '{"chain":"TRON","tx_hash":"0xabc...","log_index":0,
       "user_id":42,"asset":"USDT","amount_atoms":100000000000,
       "from_address":"T...","to_address":"T..."}'
# → {"credited":true,"new_balance_atoms":100000000000,"tx_hash":"0xabc..."}
# 重放同一 tx → {"credited":false,...}（幂等,余额不变）
```
`amount_atoms` 是 1e-8 单位(链上服务换算一次)；入账后发 AccountSet 帧,
Redis 与各 desk 自动收敛；走 fund_audit append-only 留痕。**与撮合解耦**,
链上团队对接此一个窄接口即可。每日对账:Σ虚拟账户 USDT = 链上钱包总额。

## 提现接口（两段式：用户申请 + 链上服务确认）
**Phase 1 — 用户申请**(JWT):冻结 amount + fee,记 `pending` 行,**不扣余额**。
```bash
curl -s localhost:4003/api/withdrawals -H "Authorization: Bearer $TOKEN"   -d '{"asset":"USDT","chain":"TRON","to_address":"T...","amount_atoms":500000000000}'
# → {"id":123,"status":"pending","fee_atoms":100000000}
```
**Phase 2 — 链上服务推进**(service token):风控/签名出账后调用。
```bash
# 审核通过 → 广播 → 确认(链上成交,扣减冻结)
curl /api/withdrawals/123/status -H "Authorization: Bearer $DEPOSIT_TOKEN" -d '{"status":"approved"}'
curl /api/withdrawals/123/status -H "Authorization: Bearer $DEPOSIT_TOKEN" -d '{"status":"broadcast","tx_hash":"0x..."}'
curl /api/withdrawals/123/status -H "Authorization: Bearer $DEPOSIT_TOKEN" -d '{"status":"confirmed","tx_hash":"0x..."}'
# 或失败 → 释放冻结
curl /api/withdrawals/123/status -H "Authorization: Bearer $DEPOSIT_TOKEN" -d '{"status":"failed","reason":"..."}'
```
**幂等**:`confirmed` 重放不二次扣减,`failed` 重放不二次释放,confirm/fail 互斥(状态机)。
**守恒**:每笔 `wd_freeze` 必有等额 `wd_release` 或 `wd_debit`。冻结在申请时锁定,
确认才真正离开交易所。`WITHDRAW_FEE_ATOMS` 配手续费(默认 1 USDT)。
查询:`GET /api/withdrawals`。

## 沙箱
testnet 本身即沙箱：资金来自注册种子 + `/api/test-funds` + admin 调账，
无真实资金风险。
