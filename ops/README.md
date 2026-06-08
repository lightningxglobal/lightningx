# 运维配置 (ops)

## Grafana 仪表盘
`grafana/lightning_swap_dashboard.json` — 导入 Grafana（Dashboards →
Import → Upload JSON），选择 VictoriaMetrics 的 Prometheus 数据源。
8 个面板分四组：撮合吞吐/延迟、可靠性/持久化/HA、风控/指数/标记价。

指标来源：desk-server 的 `/metrics`（API 端口）、exchange-engine
`ENGINE_METRICS_ADDR`、pg-writer `PG_WRITER_METRICS_ADDR`。让
VictoriaMetrics 抓这三个端点即可。

## 告警规则
`alerts/exchange_alerts.yml` — vmalert 规则，对应 runbook §10 告警表。

```bash
vmalert -rule=ops/alerts/exchange_alerts.yml \
        -datasource.url=http://victoriametrics:8428 \
        -notifier.url=http://alertmanager:9093
```

page = 立即呼叫（数据丢失/无主/对账违规/指数冻结）；
warn = 工作时间处理（fencing 残留/熔断/拒单尖峰/异常源）。
