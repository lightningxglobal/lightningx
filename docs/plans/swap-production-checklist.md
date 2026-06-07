# Swap（永续合约）生产化施工清单

> 创建:2026-06-07,基线 HEAD `8bad45e`。**边做边核对,完成一项勾一项。**
>
> **范围**:仅 swap(永续)。期货交割**本轮不做**——swap 完成后再加:
> 复用全部基建(持仓/保证金/强平/指数价),增量仅为合约生命周期
> (上市/到期)、交割价 TWAP、到期批量结算 sweep(零和,与 funding
> 同构),预估 2-3 周,见文末 S9 占位。
> 现货/margin/钱包/链上/KYC 不在清单(定位+延后决定,见 deferred-todos.md)。
>
> **每项的完成定义(DoD)**:代码 + 单元测试 + 集成测试 + 热路径无 ≥O(n) +
> 无明显性能衰减 + 独立 commit(descriptive)。勾选时附 commit 哈希。

---

## S0. 已验证基线(参考,全部 ✅)

- [x] 撮合引擎:整数化、确定性、GTC/IOC/FOK/PostOnly(混沌:5 杀簿逐档一致)
- [x] 账本:i64 atoms、单事务结算、守恒 property 测试、append-only 审计
- [x] 可靠性:双 journal、exactly-once、100 杀零丢失、三方对账 diff=0
- [x] HA:选主+fencing、RTO 实测 3.05s、journal-replicate 引导、运维手册
- [x] 合约风控(内存态):保证金预留/释放、持仓+已实现 PnL、破产价、
      10ms 维持保证金扫描、强平 IOC 全链路、账户状态机、保险基金记账
- [x] 安全:密钥文件化、HMAC、JWT 吊销、审计日志、metrics、fuzz

---

## S1. 持仓/保证金持久化(P0,最大的洞)

> 现状:AccountRiskState/PositionRiskState/保险基金全在内存 DashMap,
> desk 重启=持仓蒸发。复用 persist 流 + journal + 位点框架。

- [ ] S1.1 PG schema:`positions` 表(user_id, symbol, side, qty_lots,
      entry_ticks, leverage, used_margin, …)+ `risk_accounts` 表
      (equity, used/order/maintenance margin, status)+ `insurance_fund`
      单行表;非负/一致性 CHECK;迁移 022
- [ ] S1.2 PersistFrame 新增 `PositionUpsert` / `RiskAccountSet` /
      `InsuranceFundSet` 帧(152B 内布局,decoder 进 fuzz 列表)
- [ ] S1.3 desk on_fill / 强平结算路径发布持仓帧(与现有 drain 线程
      seq 体系共用);pg-writer 落库(同事务位点,exactly-once)
- [ ] S1.4 desk 启动 hydrate:从 PG 重建 risk DashMap(含保险基金);
      journal 补缺(复用 redis-writer 同款 replay 模式)
- [ ] S1.5 reconcile 扩展:内存持仓 vs PG 持仓漂移检查;
      Σ(持仓多空) 每 symbol 净额 = 0 不变量
- [ ] S1.6 集成测试:开仓→kill -9 desk→重启→持仓/equity/保险基金
      与 kill 前一致→可正常平仓(进 chaos 系列:`chaos_position_persist`)
- [ ] S1.7 演练:持仓状态下跑 `chaos_gate2_writers` 模式的轮换杀,
      三方对账含 positions 表

## S2. 单位制统一(P0,趁状态模型在动手术时做)

> 现状:risk 引擎全 cents(1e-2),账本全 atoms(1e-8),边界换算散落。

- [ ] S2.1 盘点所有 cents↔atoms 换算点(grep 审计,产出清单进本文件)
- [ ] S2.2 决策记录:risk 内部保留 cents(性能/量程)还是统一 atoms
      (一致性)——倾向统一 atoms,i64 量程对 USDT 保证金足够(±922 亿)
- [ ] S2.3 实施 + 换算边界守卫测试(任何残留换算点有 property 测试钉住)
- [ ] S2.4 守恒不变量升级为合约语义:**Σ已实现 PnL + Σ手续费 +
      Σ资金费 + 保险基金变动 = 0**(零和检验,扩展 settle_conservation)

## S3. 资金费率 funding(P0,永续的锚)

- [ ] S3.1 溢价指数:premium = (标记价 − 指数价)/指数价 的周期 TWAP
      (依赖 S4 的指数价;S4 未就绪前可先用盘口中间价占位,接口隔离)
- [ ] S3.2 费率计算:funding_rate = clamp(premium TWAP + 利率项, ±上限)
      (参数 env 化:周期、上限、利率)
- [ ] S3.3 结算 sweep:费率周期到点,按持仓名义价值批量划转
      多空双方(单事务批次 + fund_audit 流水 + 幂等键防重复结算)
- [ ] S3.4 不变量:每次 funding 结算 Σ收 = Σ付(精确到原子,集成测试)
- [ ] S3.5 重启正确性:结算周期跨 desk 重启不丢不重(持久化下次结算时间)
- [ ] S3.6 API/WS:当前费率与下次结算时间查询;历史费率表

## S4. 指数价格与标记价格(P0,反操纵)

- [ ] S4.1 指数服务接口:可插拔价格源 trait + 多源中位数聚合 +
      异常源剔除(偏离>阈值)+ 源失效降级策略(源<2 时冻结标记价更新并告警)
- [ ] S4.2 标记价格 = 指数价 + 基差的有界修正(clamp 到指数价 ±α%),
      替换现在的"盘口中间价直接当标记价"
- [ ] S4.3 强平/未实现 PnL/funding 全部切到标记价(审计所有 mark_price 调用点)
- [ ] S4.4 测试:操纵场景仿真——本所盘口被打飞 ±20% 而指数价不动,
      断言不触发强平(标记价被钳制)
- [ ] S4.5 价格源运维:源配置 env 化、源健康 metrics、手册补一节

## S5. 触发单(止损/止盈)(P0,用户侧风控)

- [ ] S5.1 数据模型:trigger 单(trigger_price, 方向, 触发后变成
      limit/market)PG 表 + API/WS 下单撤单入口
- [ ] S5.2 触发引擎:desk 侧按**标记价**(S4)监控;价格穿越触发 →
      注入普通订单进撮合;O(log n) 价格索引(按 trigger 价排序的有序结构,
      每 tick 只看穿越区间,禁全表扫)
- [ ] S5.3 触发的 exactly-once:触发动作持久化(状态机 pending→triggered),
      desk 重启不丢单不双触发(集成测试含 kill -9)
- [ ] S5.4 风控接线:触发生成的订单照常走保证金检查;失败的处置策略(撤销+通知)
- [ ] S5.5 测试:触发风暴(同价位大量触发单)性能;混沌(触发瞬间杀 desk)

## S6. 强平完善(P1,上线后第一批)

- [ ] S6.1 分档强平:先部分减仓恢复保证金率,而非全仓一刀(参数化档位)
- [ ] S6.2 ADL:保险基金穿仓时按杠杆+盈利排序自动减对手仓;ADL 指示灯 API
- [ ] S6.3 杠杆分层风险限额:持仓名义价值越大→最大可用杠杆越低(分档表)
- [ ] S6.4 强平专项混沌:标记价阶跃 → 批量强平 → kill -9 desk 中途 →
      重启后强平继续且不重复

## S7. 经济参数(P1)

- [ ] S7.1 maker/taker 费率表(per-symbol,env/表驱动),接入 on_fill 手续费腿
- [ ] S7.2 保险基金注入流水化(强平价差入金的 fund_audit 记录)+ 余额 API
- [ ] S7.3 合约参数管理:每 symbol 的 tick/lot/杠杆上限/维持保证金率
      集中到一张配置表(替代散落 env),变更带审计

## S8. 上线 Gate(全部 P0 完成后)

- [ ] S8.1 **负载压测(journal 开启,file.sync.level=2)**:目标吞吐下
      p99 延迟报告;不达标则评估 sync level 降级与 RPO 重新论证
      ——这项**现在就可以做**,不依赖 S1-S5
- [ ] S8.2 合约语义混沌全家桶:带持仓+funding+触发单状态跑
      100 杀演练,三方对账 diff=0
- [ ] S8.3 守恒终极测试:随机交易+强平+funding 长跑,零和不变量精确成立
- [ ] S8.4 双机部署:按 ha-runbook §9 上线,§12 验收表全填(跨机 RTO 实测)
- [ ] S8.5 渗透测试(延后项,真实资金前必须回来做)

---

## 进度速记

| 段 | 状态 | 完成 commit |
|---|---|---|
| S1 持仓持久化 | ⬜ 未开始 | |
| S2 单位制统一 | ⬜ 未开始 | |
| S3 funding | ⬜ 未开始 | |
| S4 指数/标记价 | ⬜ 未开始 | |
| S5 触发单 | ⬜ 未开始 | |
| S6 强平完善 | ⬜ 未开始 | |
| S7 经济参数 | ⬜ 未开始 | |
| S8 上线 Gate | ⬜ 未开始(S8.1 可立即做) | |

**建议顺序:S1 → S2 →(S3 ∥ S4)→ S5 → S8.1 随时插入 → S6/S7 → S8。**

---

## S9. 期货(交割合约)——swap 完成后的下一期(占位,本轮不做)

> 判断:**增量确实不大**。持仓/保证金/强平/指数价/触发单全部直接复用;
> 期货与 swap 的差异仅在:**没有 funding、多一个到期生命周期**。

- [ ] S9.1 合约生命周期:instrument 表(上市/交易中/待交割/已交割),
      到期后拒新开仓只允许平仓
- [ ] S9.2 交割价:到期前 N 分钟指数价 TWAP(复用 S4 指数服务)
- [ ] S9.3 到期结算 sweep:全部持仓按交割价强制平仓结算
      (批量+幂等+零和不变量——与 S3.3 funding sweep 同构,直接套框架)
- [ ] S9.4 基差监控:期现价差 metrics(临近交割收敛性)
- [ ] S9.5 同 symbol 多到期(当季/次季)的流/簿隔离
      (orders_stream_for_symbol 已按 symbol 分流,instrument id 进 symbol 命名即可)
