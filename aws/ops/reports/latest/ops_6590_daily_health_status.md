# ops 6590 - daily health status (read-only), 2026-10-21 11:25 UTC

**Status:** success  
**Duration:** 3.3s  
**Finished:** 2026-10-21T11:26:00+00:00  

## Data

| anomaly_held | budget_actual | budget_forecast | cap | capped | due_engines | fresh | held_lambdas | missing | stage | stale_free | stale_held | storage_day | storage_versioning | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1039.144 | 1192.271 | 600.0 | 5 | 162 | 142 | 5 | 1 | NORMAL | 15 | 0 | None | None | 1.5.1 |

## Log
## governor

- `11:25:58` version 1.5.1, mode enforce, stage NORMAL since 2026-10-21T00:29:25.519278+00:00, suppress_until 2026-11-01T00:29:25.519278+00:00
- `11:25:58` cap in force $600.0 (budget); budget limit $600.0, actual $1039.144, forecast $1192.271; per-engine cap $50.0/month
- `11:25:58` thresholds: {"cap": 600.0, "mtd": 1039.14, "days_left": 10.52, "baseline_usd_day": 23.92, "residual_usd_day": 7.54, "lag_buffer": 23.92, "reserve": 113.32, "freeze_at": 486.68, "brake_at": 438.84, "freeze_line_floored": false, "projected_eom": 1314.79, "frozen_eom": 1142.46, "cap_at_risk_even_frozen": true}
- `11:25:58` held by stage/lever: {"CAP/lambda": 5}
- `11:25:58` CAP held: ["justhodl-event-coordinator", "justhodl-miss-calibrator", "macro-financial-intelligence", "ofrapi", "treasury-api"]
- `11:25:58` ANOM held: []
- `11:25:58` anomalies record: {"holds": {}, "month": "2026-10", "suspects": {}}
- `11:25:58` fuse: {"checked_at": "2026-10-21T11:07:38.105248+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.34, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-cycle-features", 0.09], ["justhodl-cq-feed", 0.05], ["justhodl-symdir", 0.05], ["justhodl-apac-leadlag", 0.04], ["justhodl-signal-scorecard", 0.02], ["justhodl-sdmx-walker", 0.02], ["justhodl-episode-reference", 0.01], ["justhodl-earnings-sentiment", 0.01]]}
- `11:25:58` last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 24 functions ran, fleet $0.34/day; month-to-date top: justhodl-miss-calibrator $1551.28, justhodl-event-coordinator $431.73, justhodl-symdir $19.66"], "mode": "enforce", "mtd": 1039.14, "reason": "suppressed by operator release", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 23.92, "brake_at": 438.75, "budget_forecast": 1192.271, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": true, "cap_source": "budget", "days_left": 10.54, "freeze_at": 486.59, "freeze_line_floored": false, "frozen_eom": 1142.56, "lag_buffer": 23.92, "mtd": 1039.14, "projected_eom": 1315.1, "reserve": 113.41, "residual_source": "min7", "residual_usd_day": 7.54, "sources": {"budget_actual": 1039.144, "ce_mtd": 1039.1443}}}
- `11:25:58` cadence: {"checked": 819, "checked_at": "2026-10-21T00:07:38.381077+00:00", "day": "2026-10-21", "errors": [], "violators": []} (fixed 0)
- `11:25:58` storage: day None, versioning None, warn_days None, trip_days None
- `11:25:58` storage last assessment: null
- `11:25:58` storage s3_usd (by day): null
- `11:25:58` service spend (by day): null
- `11:25:58` meter record: {"active": 24, "daily": {"2026-10-20": {"justhodl-guardrail-notify": [3.0, 5.4e-05], "justhodl-spend-governor": [7.0, 0.000866]}, "2026-10-21": {"alphavantage-market-agent": [2.0, 2.2e-05], "bea-economic-agent": [3.0, 8e-06], "benzinga-news-agent": [3.0, 8e-06], "bls-labor-agent": [1.0, 5.1e-05], "bond-indices-agent": [1.0, 0.000262], "census-economic-agent": [1.0, 3e-05], "cftc-futures-positioning-agent": [3.0, 5.5e-05], "chatgpt-agent-api": [1.0, 0.0], "dollar-strength-agent": [4.0, 0.001218], "enhanced-repo-agent": [2.0, 1e-05], "fedliquidityapi": [3.0, 0.000137], "fmp-fundamentals-agent": [19.0, 0.000118], "fred-ice-bofa-api": [2.0, 0.0], "global-liquidity-agent-v2": [1.0, 0.0], "google-trends-agent": [3.0, 1.7e-05], "justhodl-a2a-bus": [2.0, 9.7e-05], "justhodl-ai": [3.0, 0.003781], "
## meter - month-to-date top engines

- `11:25:58`   $1551.282  justhodl-miss-calibrator
- `11:25:58`   $ 431.729  justhodl-event-coordinator
- `11:25:58`   $  19.665  justhodl-symdir
- `11:25:58`   $   4.265  justhodl-ai
- `11:25:58`   $   3.310  justhodl-tradingview
- `11:25:58`   $   2.764  justhodl-sdmx-walker
- `11:25:58`   $   2.251  justhodl-provider-catalog
- `11:25:58`   $   2.248  justhodl-polygon-options-flow
- `11:25:58`   $   2.065  justhodl-calls-research-audit
- `11:25:58`   $   1.907  justhodl-news-velocity
- `11:25:58`   $   1.726  justhodl-worldbank-full
- `11:25:58`   $   1.584  justhodl-fleet-error-monitor
- `11:25:58`   $   1.369  justhodl-ecb-deep
- `11:25:58`   $   1.245  justhodl-13f-positions
- `11:25:58`   $   1.099  justhodl-katlin
- `11:25:58` engine cap overrides: {}; released this month: {}
## holds (from the ledger, read-only)

- `11:25:58` CAP (5): justhodl-event-coordinator, justhodl-miss-calibrator, macro-financial-intelligence, ofrapi, treasury-api
## freshness since the thaw (2026-10-21 00:29 UTC)

- `11:25:59` daily schedules assessed: 717 engines; due since the thaw and before 10:40: 162 (with declared json outputs: 158, files 232); skipped {"rate": 43, "monthly": 8, "weekly-not-today": 50}
- `11:26:00` written since the thaw: 142 engines
- `11:26:00` due but not written, held by the governor (0; not re-invoked): 
- `11:26:00` due but not written, NOT held (15): cftc-futures-positioning-agent (last 10-16 06:57, due 06:48), justhodl-analytics-snapshot (last 09-03 09:02, due 09:00), justhodl-brief-compiler (last 10-16 22:20, due 01:40,01:50,15:20,17:10,18:25,18:35,22:20), justhodl-crypto-funding (last 09-09 04:43, due 04:57), justhodl-earnings-pead (last 10-15 19:48, due 08:00), justhodl-earnings-whisper (last 10-12 08:00, due 08:00), justhodl-event-flow-monitor (last 10-14 14:01, due 05:00), justhodl-fleet-integrity (last 08-06 05:01, due 05:00), justhodl-page-ai (last 09-27 08:15, due 05:10), justhodl-pead-detector (last 10-15 19:54, due 08:00), justhodl-position-sizer (last 09-29 23:32, due 06:09), justhodl-signal-genealogy (last 10-07 13:35, due 06:40), justhodl-signal-harvester (last 10-16 05:14, due 09:52,23:15), justhodl-symdir (last 10-16 22:52, due 05:40,13:46,22:05,22:29,22:44), justhodl-tv-bars (last 08-15 12:05, due 02:30)
- `11:26:00` declared outputs missing from the live bucket (1 engines): justhodl-ai ['ai/catalog.json:ClientError', 'ai/control.json:ClientError']
- `11:26:00` oldest among the stale: justhodl-fleet-integrity 76.3d, justhodl-tv-bars 67.0d, justhodl-analytics-snapshot 48.1d, justhodl-crypto-funding 42.3d, justhodl-page-ai 24.1d, justhodl-position-sizer 21.5d, justhodl-signal-genealogy 13.9d, justhodl-earnings-whisper 9.1d, justhodl-event-flow-monitor 6.9d, justhodl-earnings-pead 5.7d
- `11:26:00` ⚠ 15 due engines have not written since the thaw and are not held; 1 have declared outputs missing
- `11:26:00` ✅ status read; nothing written, nothing invoked but the governor's read-only actions
