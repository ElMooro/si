# ops 6592 - alert triage, part two (read-only)

**Status:** success  
**Duration:** 10.3s  
**Finished:** 2026-10-21T13:26:37+00:00  

## Data

| anomaly_held | api_concurrency | diag_sampled | integrity_new | undeclared |
|---|---|---|---|---|
| [["justhodl-wl-series-api", "2026-10-21T19:07:37.810878+00:00"]] | 0 | 500 | 263 | 21 |

## Log
## a. who fired the beacon

- `13:26:30` sample of 500 diag lines (query Complete): by user agent [["Lightpanda/1.0", 500]]
- `13:26:30` by page: [["/forensic.html", 146], ["/bottom.html", 122], ["/why.html", 114], ["/share-flows.html", 105], ["/statement-research.html", 12], ["/engine-data.html", 1]]
- `13:26:32` all diag lines by user agent (first 40 chars, query Complete): [["Lightpanda/1.0\n", 8937], ["Mozilla/5.0 (X11; Linux x86_64) AppleWeb", 535], ["Mozilla/5.0 (compatible; ExaSearchBot/1.", 6], ["Mozilla/5.0 AppleWebKit/537.36 (KHTML, l", 6]]
- `13:26:34` (sanity) Complete
## b. the hold

- `13:26:36` stage NORMAL; anomaly_held [["justhodl-wl-series-api", "2026-10-21T19:07:37.810878+00:00"]]; held {"CAP/lambda": 5, "ANOM/lambda": 1}; capped ["justhodl-event-coordinator", "justhodl-miss-calibrator", "macro-financial-intelligence", "ofrapi", "treasury-api"]; anomalies {"holds": {"justhodl-wl-series-api": 1}, "month": "2026-10", "suspects": {"justhodl-edgar-insiders": "2026-10-21T13:07:37.810878+00:00", "justhodl-fundamental-graphs": "2026-10-21T13:07:37.810878+00:00"}}
- `13:26:36` justhodl-wl-series-api reserved concurrency now: 0
## c. the 21 undeclared live triggers

- `13:26:37` undeclared (21): [{"key": "justhodl-chain-resumer-daily", "err": "ParamValidationError"}, {"key": "justhodl-dex-scanner-daily", "err": "ParamValidationError"}, {"key": "justhodl-katlin-permission-refresh", "err": "ParamValidationError"}, {"key": "justhodl-price-redundancy-daily", "err": "ParamValidationError"}, {"key": "justhodl-student-rsi-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-esma-ratings-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-fhfa-hpi-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-engine-registry-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-fragile-states-index-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-cds-desk-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-world-uncertainty-index-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-corp-bonds-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-epu-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-fao-food-price-index-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-risk-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-worldbank-wgi-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-eba-risk-dashboard-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-nyfed-hhdc-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-spend-governor-tick", "err": "ParamValidationError"}, {"key": "default/justhodl-fdic-bankfind-daily", "err": "ParamValidationError"}, {"key": "default/justhodl-nyfed-cmdi-daily", "err": "ParamValidationError"}]
## d. fleet-integrity new defects

- `13:26:37` new 263 by class: [["D8_errors", 170], ["D7_concurrency_drop", 58], ["D4_orphan_rule", 20], ["D14_unscanned", 6], ["D6_scheduled_never_ran", 5], ["D2_timeout_clipped", 4]]
- `13:26:37` sev1 rows sample: [{"cls": "D2_timeout_clipped", "sev": 1, "id": "justhodl-analytics-snapshot", "detail": "avg 456s of a 600s ceiling \u2014 the tail of every run is being cut off", "is_new": true}, {"cls": "D2_timeout_clipped", "sev": 1, "id": "justhodl-causality-scanner", "detail": "avg 558s of a 600s ceiling \u2014 the tail of every run is being cut off", "is_new": true}, {"cls": "D2_timeout_clipped", "sev": 1, "id": "justhodl-imf-full", "detail": "avg 850s of a 850s ceiling \u2014 the tail of every run is being cut off", "is_new": true}, {"cls": "D2_timeout_clipped", "sev": 1, "id": "justhodl-outcome-checker", "detail": "avg 736s of a 900s ceiling \u2014 the tail of every run is being cut off", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "calibration-snapshotter-weekly", "detail": "targets missing function live (cron(0 4 ? * SUN *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "fleet-freshness-monitor-30min", "detail": "targets missing function live (cron(57 7 * * ? *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "fleet-freshness-monitor-sched", "detail": "targets missing function live (cron(26 7 * * ? *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "justhodl-backtest-engine-6h", "detail": "targets missing function live (cron(30 22 * * ? *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "justhodl-engine-fusion-hourly", "detail": "targets missing function live (cron(15 2 * * ? *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "justhodl-fleet-freshness-hourly", "detail": "targets missing function live (cron(59 1 * * ? *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "justhodl-fleet-freshness-monitor-sched", "detail": "targets missing function live (cron(56 1 * * ? *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "justhodl-freshness-30m", "detail": "targets missing function live (cron(34 8 * * ? *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "justhodl-katlin-backtest-weekly", "detail": "targets missing function live (cron(30 9 ? * SUN *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "justhodl-katlin-daily", "detail": "targets missing function live (cron(10 4 ? * TUE-SAT *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "justhodl-katlin-permission-refresh", "detail": "targets missing function live (cron(0 13 ? * MON-FRI *))", "is_new": true}, {"cls": "D4_orphan_rule", "sev": 1, "id": "justhodl-katlin-
- `13:26:37` ✅ triage part two read
