# ops 6606 - daily health status (read-only), 2026-10-22 11:23 UTC

**Status:** failure  
**Duration:** 2.7s  
**Finished:** 2026-10-22T11:23:33+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6606_daily_health_status.py", line 159, in main
    skipped["weekly-not-today" if kind == "weekly" else kind] += 1
    ~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
KeyError: 'daily'

```

## Log
## governor

- `11:23:32` version 1.5.1, mode enforce, stage NORMAL since 2026-10-21T00:29:25.519278+00:00, suppress_until 2026-11-01T00:29:25.519278+00:00
- `11:23:32` cap in force $600.0 (budget); budget limit $600.0, actual $1063.39, forecast $1280.666; per-engine cap $50.0/month
- `11:23:32` thresholds: {"cap": 600.0, "mtd": 1063.39, "days_left": 9.53, "baseline_usd_day": 22.79, "residual_usd_day": 2.11, "lag_buffer": 22.79, "reserve": 52.88, "freeze_at": 547.12, "brake_at": 501.53, "freeze_line_floored": false, "projected_eom": 1303.3, "frozen_eom": 1106.27, "cap_at_risk_even_frozen": true}
- `11:23:32` held by stage/lever: {"CAP/lambda": 5}
- `11:23:32` CAP held: ["justhodl-event-coordinator", "justhodl-miss-calibrator", "macro-financial-intelligence", "ofrapi", "treasury-api"]
- `11:23:32` ANOM held: []
- `11:23:32` anomalies record: {"errors_notified": {"day": "2026-10-21", "fns": ["justhodl-polygon-futures-curves"]}, "holds": {"justhodl-wl-series-api": 1}, "month": "2026-10", "suspects": {}}
- `11:23:32` fuse: {"checked_at": "2026-10-22T11:07:37.854194+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.27, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-apac-leadlag", 0.08], ["justhodl-cq-feed", 0.06], ["justhodl-cycle-features", 0.05], ["justhodl-signal-scorecard", 0.02], ["justhodl-sdmx-walker", 0.02], ["justhodl-earnings-sentiment", 0.01], ["justhodl-episode-reference", 0.01], ["justhodl-regime-anomaly", 0.01]]}
- `11:23:32` last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 19 functions ran, fleet $0.27/day; month-to-date top: justhodl-miss-calibrator $1551.28, justhodl-event-coordinator $431.73, justhodl-symdir $20.04"], "mode": "enforce", "mtd": 1063.39, "reason": "suppressed by operator release", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 22.79, "brake_at": 501.51, "budget_forecast": 1280.666, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": true, "cap_source": "budget", "days_left": 9.54, "freeze_at": 547.1, "freeze_line_floored": false, "frozen_eom": 1106.29, "lag_buffer": 22.79, "mtd": 1063.39, "projected_eom": 1303.55, "reserve": 52.9, "residual_source": "min7", "residual_usd_day": 2.11, "sources": {"budget_actual": 1063.39, "ce_mtd": 1063.3897}}}
- `11:23:32` cadence: {"checked": 824, "checked_at": "2026-10-22T00:07:38.400364+00:00", "day": "2026-10-22", "errors": [], "violators": []} (fixed 0)
- `11:23:32` storage: day 2026-10-21, versioning None, warn_days None, trip_days None
- `11:23:32` storage last assessment: {"day": "2026-10-20", "delta_gb": -887.62, "delta_objects": -3981806, "delta_pct": -83.63, "gb": 173.704, "level": null, "objects": 0, "prev": "2026-10-19", "prev_gb": 1061.32, "reason": "growth -887.6 GB (-83.6%), -3981806 objects over 2026-10-20"}
- `11:23:32` storage days (last 5): []
- `11:23:32` storage s3_usd (by day): {"2026-10-18": 12.714, "2026-10-19": 8.4935, "2026-10-20": 12.8342}
- `11:23:32` service spend (by day): {"2026-10-18": {"AWS Cost Explorer": 0.04, "AWS Key Management Service": 0.0, "AWS Lambda": 0.0255, "AWS Secrets Manager": 0.1161, "AWS Systems Manager": 0.0101, "Amazon API Gateway": 0.0, "Amazon DynamoDB": 0.0232, "Amazon EC2 Container Registry (ECR)": 0.0875, "Amazon Elastic File System": 0.0, "Amazon SageMaker": 0.0181, "Amazon Simple Notification Service": 0.0, "Amazon Simple Queue Service": 0.0031, "Amazon Simple Storage Service": 12.714, "AmazonCloudWatch": 0.1873, "EC2 - Other": 0.0413}, "2026-10-19": {"AWS Business Support+": 11.5846, "AWS Cost Explorer": 0.09, "AWS Key Management Service": 0.0003, "AWS Lambda": 0.0256, "AWS Secrets Manager": 0.1161, "AWS Step Functions": 0.0, "AWS Systems Manager": 0.0101, "Amazon API Gateway": 0.0, "Amazon DynamoDB": 0.0223, "Amazon EC2 Container Registry (ECR)": 0.0875, "Amazon Elastic File System": 0.0, "Amazon SageMaker": 0.0181, "Amazon Simple Notification Service": 0.0, "Amazon Simple Queue Service": 0.0034, "Amazon Simple Storage Service": 8.4935, "AmazonCloudWatch": 0.2645, "EC2 - Other": 0.0413}, "2026-10-20": {"AWS Cost Explorer": 0.21, "AWS Key Management Service": 0.0001, "AWS Lambda": 0.0277, "AWS Secrets Manager": 0.1161, "AWS Step Functions": 0.0, "AWS Systems Manager": 0.0101, "Amazon DynamoDB": 0.0221, "Amazon EC2 Container Registry (ECR)": 0.0875, "Amazon Elastic File System": 0.0, "Amazon SageMaker": 0.0181, "Amazon Simple Notification Service": 0.0, "Amazon Simple Queue Service": 0.0026, "Amazon Simple Storage Service": 12.8342, "AmazonCloudWatch": 0.3677, "EC2 - Other": 0.0413}}
- `11:23:32` meter record: {"active": 19, "daily": {"2026-10-20": {"justhodl-guardrail-notify": [3.0, 5.4e-05], "justhodl-spend-governor": [7.0, 0.000866]}, "2026-10-21": {"MLPredictor": [1.0, 0.0], "alphavantage-market-agent": [6.0, 6.4e-05], "alphavantage-technical-analysis": [2.0, 2e-06], "bea-economic-agent": [6.0, 1.8e-05], "benzinga-news-agent": [3.0, 8e-06], "bls-employment-api-v2": [1.0, 3.8e-05], "bls-labor-agent": [4.0, 0.000234], "bond-indices-agent": [5.0, 0.001124], "census-economic-agent": [4.0, 0.000227], "cftc-futures-positioning-agent": [7.0, 0.001363], "chatgpt-agent-api": [3.0, 0.0], "coinmarketcap-agent": [2.0, 1.2e-05], "daily-liquidity-report": [1.0, 0.000104], "dollar-strength-agent": [6.0, 0.001948], "eia-energy-agent": [1.0, 0.000662], "enhanced-repo-agent": [6.0, 9.6e-05], "fedliquidityapi"
## meter - month-to-date top engines

- `11:23:32`   $1551.282  justhodl-miss-calibrator
- `11:23:32`   $ 431.729  justhodl-event-coordinator
- `11:23:32`   $  20.036  justhodl-symdir
- `11:23:32`   $   4.269  justhodl-ai
- `11:23:32`   $   3.337  justhodl-tradingview
- `11:23:32`   $   2.765  justhodl-sdmx-walker
- `11:23:32`   $   2.281  justhodl-polygon-options-flow
- `11:23:32`   $   2.265  justhodl-provider-catalog
- `11:23:32`   $   2.068  justhodl-calls-research-audit
- `11:23:32`   $   1.913  justhodl-news-velocity
- `11:23:32`   $   1.726  justhodl-worldbank-full
- `11:23:32`   $   1.584  justhodl-fleet-error-monitor
- `11:23:32`   $   1.373  justhodl-ecb-deep
- `11:23:32`   $   1.258  justhodl-13f-positions
- `11:23:32`   $   1.118  justhodl-katlin
- `11:23:32` engine cap overrides: {}; released this month: {}
## holds (from the ledger, read-only)

- `11:23:33` CAP (5): justhodl-event-coordinator, justhodl-miss-calibrator, macro-financial-intelligence, ofrapi, treasury-api
## freshness today (declared outputs written after today's trigger minute)

