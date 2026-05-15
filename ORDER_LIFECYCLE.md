# 委托生命周期图 (Order Lifecycle)

## 1. IOC (Immediate-or-Cancel) 委托

```
发送IOC委托
    ↓
[有匹配方] ────→ 全部成交
    ↓              ↓
    │          OrderUpdate: FILLED
    │              ↓
    │          Trade: 生成
    │              ↓
    │          [完成] ✓
    │
[无匹配方] ────→ 拒绝（不进入簿）
    ↓              ↓
    │          OrderUpdate: REJECTED
    │              ↓
    │          [完成] ✓
    │
[部分匹配] ────→ 应该REJECTED（IOC要么全部要么取消）
    ↓              ↓
                OrderUpdate: REJECTED
                    ↓
                [完成] ✓

期望消息序列:
场景1（无匹配）: [NEW] → [DELETED-REJECTED]
场景2（完全匹配）: [NEW] → [TRADED-FILLED]
场景3（部分匹配）: [NEW] → [DELETED-REJECTED] (或无NEW，直接REJECTED)
```

## 2. GTC (Good-Till-Cancel) 委托

```
发送GTC委托
    ↓
OrderUpdate: ACCEPTED (进入簿)
    ↓
┌─────────────────────────────────────────────────┐
│                                                 │
[部分成交]                                    [完全成交]
  ↓                                               ↓
OrderUpdate: PARTIALLY_FILLED          OrderUpdate: FILLED
仍在簿中，等待更多                        [完成] ✓
  ↓                                    
Trade: 生成（每次部分匹配都生成）      
  ↓
可继续部分成交或被撤销
  ↓
[被撤销]
  ↓
OrderUpdate: CANCELLED
[完成] ✓

期望消息序列:
场景1（部分成交后继续成交）:
  [NEW-ACCEPTED] → [TRADED-PARTIALLY_FILLED, TRADE] → [TRADED-FILLED]

场景2（完全进入簿，无人成交，被撤销）:
  [NEW-ACCEPTED] → [DELETED-CANCELLED]

场景3（直接完全成交）:
  [NEW-ACCEPTED] → [TRADED-FILLED]
```

## 3. FOK (Fill-or-Kill) 委托

```
发送FOK委托
    ↓
[能完全成交]  ────→ 全部成交
    ↓                 ↓
    │             OrderUpdate: FILLED
    │                 ↓
    │             Trade: 生成
    │                 ↓
    │             [完成] ✓
    │
[不能完全成交] ────→ 全部拒绝
    ↓                 ↓
                 OrderUpdate: REJECTED
                     ↓
                 [完成] ✓

期望消息序列:
场景1（可完全成交）: [NEW] → [TRADED-FILLED]
场景2（不能完全成交）: [NEW] → [DELETED-REJECTED]
```

## 4. PostOnly 委托

```
发送PostOnly委托
    ↓
[不会立即成交] ────→ 进入簿
    ↓                  ↓
    │              OrderUpdate: ACCEPTED
    │                  ↓
    │              可被后续订单成交
    │              或手动撤销
    │                  ↓
    ├─ [被后续订单成交] ──→ OrderUpdate: PARTIALLY_FILLED/FILLED
    │                   Trade: 生成
    │                   [完成]
    │
    └─ [被撤销] ────────→ OrderUpdate: CANCELLED
                        [完成]

期望消息序列:
场景1（进入簿，无人成交）:
  [NEW-ACCEPTED] → [DELETED-CANCELLED]

场景2（进入簿，被后续订单成交）:
  [NEW-ACCEPTED] → [TRADED-FILLED] (或PARTIALLY_FILLED)
```

## 5. 关键规则

### OrderUpdate消息的类型：
- **ACCEPTED (NEW)**: 订单被交易所接受，进入委托簿
  - 仅当订单进入簿时生成（GTC、PostOnly等）
  
- **FILLED**: 订单全部成交，完全离开簿
  - 当剩余数量 == 0时生成
  
- **PARTIALLY_FILLED**: 订单部分成交，仍在簿中
  - 当 0 < 剩余数量 < 原始数量时生成
  - 每次部分成交都应该生成一条
  
- **REJECTED**: 订单被拒绝，不进入簿或被全部拒绝
  - IOC无匹配时
  - FOK无法完全成交时
  - 其他拒绝原因
  
- **CANCELLED**: 订单被手动撤销
  - 用户发送CancelOrder时生成

### Trade消息的生成：
- 每次两个订单配对成交时生成1条Trade
- 包含：sequence, taker_order_id, maker_order_id, price, quantity, side

### 重要观察：
1. 不是所有订单都会生成ACCEPTED
   - IOC、FOK通常不生成（除非进入簿后立即被拒或成交）
   
2. 一个订单可能生成多条OrderUpdate
   - GTC: ACCEPTED → PARTIALLY_FILLED → FILLED (3条)
   - GTC: ACCEPTED → PARTIALLY_FILLED → CANCELLED (3条)
   
3. Trade和OrderUpdate的关系
   - Trade生成时，相关的两个订单都应该收到OrderUpdate
   - 一个Trade对应：卖方订单1条Update + 买方订单1条Update
