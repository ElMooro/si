# ops 6618 - daily health status (read-only), 2026-10-23 11:34 UTC

**Status:** success  
**Duration:** 2.4s  
**Finished:** 2026-10-23T11:34:25+00:00  

## Data

| alarms_not_ok | anomaly_held | budget_actual | budget_forecast | cap | capped | due_engines | fresh | held_lambdas | missing | stage | stale_free | stale_held | storage_day | storage_versioning | storm_errors_24h | storm_invocations_24h | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 1095.305 | 1320.21 | 600.0 | 5 | 177 | 149 | 5 | 1 | NORMAL | 14 | 0 | 2026-10-22 | None | 44 | 92 | 1.5.1 |

## Log
## governor

- `11:34:23` version 1.5.1, mode enforce, stage NORMAL since 2026-10-21T00:29:25.519278+00:00, suppress_until 2026-11-01T00:29:25.519278+00:00
- `11:34:23` cap in force $600.0 (budget); budget limit $600.0, actual $1095.305, forecast $1320.21; per-engine cap $50.0/month
- `11:34:23` thresholds: {"cap": 600.0, "mtd": 1095.31, "days_left": 8.52, "baseline_usd_day": 23.19, "residual_usd_day": 0.58, "lag_buffer": 23.19, "reserve": 38.16, "freeze_at": 561.84, "brake_at": 515.47, "freeze_line_floored": false, "projected_eom": 1315.98, "frozen_eom": 1123.46, "cap_at_risk_even_frozen": true}
- `11:34:23` held by stage/lever: {"CAP/lambda": 5}
- `11:34:23` CAP held: ["justhodl-event-coordinator", "justhodl-miss-calibrator", "macro-financial-intelligence", "ofrapi", "treasury-api"]
- `11:34:23` ANOM held: []
- `11:34:23` anomalies record: {"errors_notified": {"day": "2026-10-22", "fns": ["justhodl-polygon-futures-curves"]}, "holds": {"justhodl-wl-series-api": 1}, "month": "2026-10", "suspects": {}}
- `11:34:23` fuse: {"checked_at": "2026-10-23T11:07:37.801675+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.3, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-apac-leadlag", 0.07], ["justhodl-cq-feed", 0.05], ["justhodl-cycle-features", 0.05], ["justhodl-sdmx-walker", 0.03], ["justhodl-fundamental-graphs", 0.02], ["justhodl-signal-scorecard", 0.02], ["justhodl-earnings-sentiment", 0.02], ["justhodl-episode-reference", 0.01]]}
- `11:34:23` last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 22 functions ran, fleet $0.30/day; month-to-date top: justhodl-miss-calibrator $1551.28, justhodl-event-coordinator $431.73, justhodl-symdir $20.28"], "mode": "enforce", "mtd": 1095.31, "reason": "suppressed by operator release", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 23.19, "brake_at": 515.46, "budget_forecast": 1320.21, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": true, "cap_source": "budget", "days_left": 8.54, "freeze_at": 561.83, "freeze_line_floored": false, "frozen_eom": 1123.48, "lag_buffer": 23.19, "mtd": 1095.31, "projected_eom": 1316.41, "reserve": 38.17, "residual_source": "min7", "residual_usd_day": 0.58, "sources": {"budget_actual": 1095.305, "ce_mtd": 1095.3047}}}
- `11:34:23` cadence: {"checked": 825, "checked_at": "2026-10-23T00:07:37.757144+00:00", "day": "2026-10-23", "errors": [], "violators": []} (fixed 0)
- `11:34:23` storage: day 2026-10-22, versioning None, warn_days None, trip_days None
- `11:34:23` storage last assessment: {"day": "2026-10-21", "delta_gb": -864.24, "delta_objects": -3807457, "delta_pct": -83.26, "gb": 173.742, "level": null, "objects": 0, "prev": "2026-10-20", "prev_gb": 1037.985, "reason": "growth -864.2 GB (-83.3%), -3807457 objects over 2026-10-21"}
- `11:34:23` storage days (last 5): []
- `11:34:23` storage s3_usd (by day): {"2026-10-18": 12.714, "2026-10-19": 8.4935, "2026-10-20": 12.8342, "2026-10-21": 18.9137}
- `11:34:23` service spend (by day): {"2026-10-18": {"AWS Cost Explorer": 0.04, "AWS Key Management Service": 0.0, "AWS Lambda": 0.0255, "AWS Secrets Manager": 0.1161, "AWS Systems Manager": 0.0101, "Amazon API Gateway": 0.0, "Amazon DynamoDB": 0.0232, "Amazon EC2 Container Registry (ECR)": 0.0875, "Amazon Elastic File System": 0.0, "Amazon SageMaker": 0.0181, "Amazon Simple Notification Service": 0.0, "Amazon Simple Queue Service": 0.0031, "Amazon Simple Storage Service": 12.714, "AmazonCloudWatch": 0.1873, "EC2 - Other": 0.0413}, "2026-10-19": {"AWS Business Support+": 11.5846, "AWS Cost Explorer": 0.09, "AWS Key Management Service": 0.0003, "AWS Lambda": 0.0256, "AWS Secrets Manager": 0.1161, "AWS Step Functions": 0.0, "AWS Systems Manager": 0.0101, "Amazon API Gateway": 0.0, "Amazon DynamoDB": 0.0223, "Amazon EC2 Container Registry (ECR)": 0.0875, "Amazon Elastic File System": 0.0, "Amazon SageMaker": 0.0181, "Amazon Simple Notification Service": 0.0, "Amazon Simple Queue Service": 0.0034, "Amazon Simple Storage Service": 8.4935, "AmazonCloudWatch": 0.2645, "EC2 - Other": 0.0413}, "2026-10-20": {"AWS Cost Explorer": 0.21, "AWS Key Management Service": 0.0001, "AWS Lambda": 0.0277, "AWS Secrets Manager": 0.1161, "AWS Step Functions": 0.0, "AWS Systems Manager": 0.0101, "Amazon DynamoDB": 0.0221, "Amazon EC2 Container Registry (ECR)": 0.0875, "Amazon Elastic File System": 0.0, "Amazon SageMaker": 0.0181, "Amazon Simple Notification Service": 0.0, "Amazon Simple Queue Service": 0.0026, "Amazon Simple Storage Service": 12.8342, "AmazonCloudWatch": 0.3677, "EC2 - Other": 0.0413}, "2026-10-21": {"AWS Cost Explorer": 0.07, "AWS Key Management Service": 0.0009, "AWS Lambda": 1.5766, "AWS Secrets Manager": 0.1161, "AWS Systems Manager": 0.0106, "Amazon API Gateway": 0.0, "Amazon DynamoDB": 0.0737, "Amazon EC2 Container Registry (ECR)": 0.0875, "Amazon Elastic File System": 0.0, "Amazon SageMaker": 0.0181, "Amazon Simple Email Service": 0.0, "Amazon Simple Notification Service": 0.0, "Amazon Simple Queue Service": 0.0001, "Amazon Simple Storage Service": 18.9137, "AmazonCloudWatch": 1.1509, "CloudWatch Events": 0.0003, "EC2 - Other": 0.0396}}
- `11:34:23` meter record: {"active": 22, "daily": {"2026-10-20": {"justhodl-guardrail-notify": [3.0, 5.4e-05], "justhodl-spend-governor": [7.0, 0.000866]}, "2026-10-21": {"MLPredictor": [1.0, 0.0], "alphavantage-market-agent": [6.0, 6.4e-05], "alphavantage-technical-analysis": [2.0, 2e-06], "bea-economic-agent": [6.0, 1.8e-05], "benzinga-news-agent": [3.0, 8e-06], "bls-employment-api-v2": [1.0, 3.8e-05], "bls-labor-agent": [4.0, 0.000234], "bond-indices-agent": [5.0, 0.001124], "census-economic-agent": [4.0, 0.000227], "cftc-futures-positioning-agent": [7.0, 0.001363], "chatgpt-agent-api": [3.0, 0.0], "coinmarketcap-agent": [2.0, 1.2e-05], "daily-liquidity-report": [1.0, 0.000104], "dollar-strength-agent": [6.0, 0.001948], "eia-energy-agent": [1.0, 0.000662], "enhanced-repo-agent": [6.0, 9.6e-05], "fedliquidityapi"
## meter - month-to-date top engines

- `11:34:23`   $1551.282  justhodl-miss-calibrator
- `11:34:23`   $ 431.729  justhodl-event-coordinator
- `11:34:23`   $  20.284  justhodl-symdir
- `11:34:23`   $   4.273  justhodl-ai
- `11:34:23`   $   3.365  justhodl-tradingview
- `11:34:23`   $   2.772  justhodl-sdmx-walker
- `11:34:23`   $   2.314  justhodl-polygon-options-flow
- `11:34:23`   $   2.271  justhodl-provider-catalog
- `11:34:23`   $   2.069  justhodl-calls-research-audit
- `11:34:23`   $   1.918  justhodl-news-velocity
- `11:34:23`   $   1.726  justhodl-worldbank-full
- `11:34:23`   $   1.585  justhodl-fleet-error-monitor
- `11:34:23`   $   1.375  justhodl-ecb-deep
- `11:34:23`   $   1.265  justhodl-13f-positions
- `11:34:23`   $   1.142  justhodl-katlin
- `11:34:23` engine cap overrides: {}; released this month: {}
## holds (from the ledger, read-only)

- `11:34:23` CAP (5): justhodl-event-coordinator, justhodl-miss-calibrator, macro-financial-intelligence, ofrapi, treasury-api
## freshness today (declared outputs written after today's trigger minute)

- `11:34:23` daily schedules assessed: 739 engines; due today before 10:49 UTC: 177 (with declared json outputs: 164, files 239); skipped {"rate": 43, "monthly": 8, "weekly-not-today": 46, "no-single-minute": 2}
- `11:34:23` schedules without a single daily minute (not assessed): jhk-tick-session-5min cron(0/5 9-16 ? * MON-FRI *); justhodl-spend-governor-tick cron(7 * * * ? *)
- `11:34:24` written after today's trigger minute: 149 engines
- `11:34:24` due but not written, held by the governor (0; not re-invoked): 
- `11:34:24` due but not written, NOT held (14): cftc-futures-positioning-agent (last 10-22 13:52, due 06:48), justhodl-analytics-snapshot (last 09-03 09:02, due 09:00), justhodl-backfill-orchestrator (last 10-23 05:55, due 06:44), justhodl-brief-compiler (last 10-22 22:20, due 01:40,01:50), justhodl-crypto-funding (last 09-09 04:43, due 04:57), justhodl-event-flow-monitor (last 10-14 14:01, due 05:00), justhodl-fleet-integrity (last 08-06 05:01, due 05:00), justhodl-gdelt-sentiment (last 10-21 03:43, due 03:43), justhodl-page-ai (last 09-27 08:15, due 05:10), justhodl-position-sizer (last 09-29 23:32, due 06:09), justhodl-signal-genealogy (last 10-07 13:35, due 06:40), justhodl-signal-harvester (last 10-22 23:17, due 09:52), justhodl-symdir (last 10-22 23:38, due 05:40), justhodl-tv-bars (last 08-15 12:05, due 02:30)
- `11:34:24` declared outputs missing from the live bucket (1 engines): justhodl-ai ['ai/catalog.json:ClientError', 'ai/control.json:ClientError']
- `11:34:24` oldest among the stale: justhodl-fleet-integrity 78.3d, justhodl-tv-bars 69.0d, justhodl-analytics-snapshot 50.1d, justhodl-crypto-funding 44.3d, justhodl-page-ai 26.1d, justhodl-position-sizer 23.5d, justhodl-signal-genealogy 15.9d, justhodl-event-flow-monitor 8.9d, justhodl-gdelt-sentiment 2.3d, cftc-futures-positioning-agent 0.9d, justhodl-brief-compiler 0.6d, justhodl-signal-harvester 0.5d
- `11:34:24` stale today but written within the last 26 h (missed only today's slot so far): cftc-futures-positioning-agent, justhodl-backfill-orchestrator, justhodl-brief-compiler, justhodl-signal-harvester, justhodl-symdir
## issue #125 - justhodl-polygon-futures-curves (read-only)

- `11:34:24` Invocations last 24 h: 92  by hour: [["10-22 12:00", 18], ["10-22 13:00", 24], ["10-22 14:00", 24], ["10-22 15:00", 23], ["10-22 16:00", 3]]
- `11:34:24` Errors last 24 h: 44  by hour: [["10-22 12:00", 9], ["10-22 13:00", 12], ["10-22 14:00", 12], ["10-22 15:00", 11]]
- `11:34:24` Throttles last 24 h: 0  by hour: []
- `11:34:24` error log lines read (last 6 h): 0
- `11:34:25` function: version $LATEST, last modified 2026-10-22T16:35:15.000+0000, state Active, reserved concurrency null
- `11:34:25` output data/polygon-futures-curves.json last modified 2026-10-22 16:48 UTC
- `11:34:25` declared output data/futures-research.json last modified 2026-10-22 16:48 UTC
- `11:34:25` declared output data/polygon-futures-curves.json last modified 2026-10-22 16:48 UTC
## governor guardrail self-check and dead-man alarms (read-only)

- `11:34:25` guardrail check (state.guardrails): {"checked_at": "2026-10-23T11:07:37.801675+00:00", "problems": []}
- `11:34:25` storage days (state, last 4): [["2026-10-18", {"gb": 1060.677, "objects": 3966082}], ["2026-10-19", {"gb": 1061.32, "objects": 3981806}], ["2026-10-20", {"gb": 1037.985, "objects": 3807457}], ["2026-10-21", {"gb": 173.742, "objects": 0}]]
- `11:34:25`   justhodl-spend-governor-heartbeat: OK (since 10-16 21:01)
- `11:34:25`   justhodl-spend-governor-errors: OK (since 10-16 21:02)
- `11:34:25`   justhodl-runaway-concurrency: OK (since 10-17 01:55)
- `11:34:25`   justhodl-runaway-invocations: OK (since 10-17 01:55)
- `11:34:25`   justhodl-runaway-duration: OK (since 10-17 01:56)
- `11:34:25`   justhodl-event-breaker-open: OK (since 10-17 01:56)
- `11:34:25`   justhodl-event-breaker-unavailable: OK (since 10-17 01:55)
- `11:34:25`   justhodl-s3-live-bucket-size: OK (since 10-18 19:38)
- `11:34:25`   justhodl-llm-providers-down: OK (since 10-22 17:36)
- `11:34:25` ⚠ 14 due engines have not written after today's trigger minute and are not held; 1 have declared outputs missing
- `11:34:25` ✅ status read; nothing written, nothing invoked but the governor's read-only actions
