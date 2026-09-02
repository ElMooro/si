# ops 5106 -- restore the fan-out cluster silenced on 2026-08-01

**Status:** success  
**Duration:** 496.3s  
**Finished:** 2026-09-02T01:26:10+00:00  

## Data

| errored | feed_moved | fn | hours | invoked | per_day | ticks |
|---|---|---|---|---|---|---|
|  |  | MLPredictor | 11 |  | 1.0 | daily-morn |
|  |  | bls-employment-api-v2 | 22 |  | 1.0 | daily-eve |
|  |  | daily-liquidity-report | 11 |  | 1.0 | daily-morn |
|  |  | ecb-data-daily-updater | 6 |  | 1.0 | daily-06utc |
|  |  | justhodl-ab-test | 16 |  | 1.0 | daily-16utc,every_6h |
|  |  | justhodl-activist-filings-scanner | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-allocator | 18 |  | 1.0 | 4hourly |
|  |  | justhodl-asymmetric-hunter | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-auction-interpreter | 18 |  | 1.0 | 4hourly |
|  |  | justhodl-bloomberg-v8 | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | justhodl-bond-regime-detector | 18 |  | 1.0 | 4hourly |
|  |  | justhodl-calls-backtest | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-correlation-breaks | 22 |  | 1.0 | daily-eve |
|  |  | justhodl-correlation-surface | 15 |  | 1.0 | daily-15utc |
|  |  | justhodl-data-collector | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | justhodl-deep-value-screener | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-dep-graph | 22 |  | 1.0 | daily-eve |
|  |  | justhodl-divergence-scanner | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-earnings-pead | 8 |  | 1.0 | daily-08utc |
|  |  | justhodl-earnings-whisper | 8 |  | 1.0 | daily-08utc |
|  |  | justhodl-email-reports-v2 | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-eps-revision-velocity | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-fed-speak | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-health-monitor | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | justhodl-khalid-metrics | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-market-interpreter | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | justhodl-microcap-float-squeeze | 22 |  | 1.0 | daily-eve |
|  |  | justhodl-ml-predictions | 18 |  | 1.0 | 4hourly |
|  |  | justhodl-momentum-breakout | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-momentum-scanner | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-morning-brief-tg | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-narrative-density-tracker | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-nobrainer-rationale | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-nobrainer-tracker | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | justhodl-options-flow-scanner | 22 |  | 1.0 | daily-eve |
|  |  | justhodl-pead-detector | 8 |  | 1.0 | daily-08utc |
|  |  | justhodl-position-monitor | 14 |  | 1.0 | 30min |
|  |  | justhodl-position-sizer-v2 | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-pre-pump-detector | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-regime-composite | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | justhodl-reports-builder | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | justhodl-revenue-acceleration | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-risk-sizer | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-sector-earnings-diffusion | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-sector-tilt | 18 |  | 1.0 | 4hourly |
|  |  | justhodl-signal-portfolio | 22 |  | 1.0 | daily-eve |
|  |  | justhodl-smart-money-cluster | 16 |  | 1.0 | daily-16utc,every_6h |
|  |  | justhodl-supply-inflection-scanner | 7 |  | 1.0 | daily-07utc |
|  |  | justhodl-synthetic-monitor | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | justhodl-tape-reader | 22 |  | 1.0 | daily-eve |
|  |  | justhodl-theme-detector | 6 |  | 1.0 | daily-06utc |
|  |  | justhodl-theme-rotation-engine | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-theme-tier-classifier | 8 |  | 1.0 | daily-08utc |
|  |  | justhodl-universe-builder | 18 |  | 1.06 | 4hourly |
|  |  | justhodl-vol-regime | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | justhodl-volatility-squeeze-hunter | 11 |  | 1.0 | daily-morn |
|  |  | justhodl-watchlist-debate | 22 |  | 1.0 | daily-eve |
|  |  | justhodl-whats-changed | 17 |  | 1.0 | 15min,daily-17utc,hourly |
|  |  | ofrapi | 11 |  | 1.88 | daily-morn |
|  |  | permanent-market-intelligence | 11 |  | 1.0 | daily-morn |
|  |  | scrapeMacroData | 11 |  | 1.0 | daily-morn |
|  |  | treasury-auto-updater | 11 |  | 1.0 | daily-morn |
|  |  | ultimate-multi-agent |  |  | 3.69 | daily-morn |
| False | False | MLPredictor |  | True |  |  |
| False | False | bls-employment-api-v2 |  | True |  |  |
| False | False | daily-liquidity-report |  | True |  |  |
| False | False | ecb-data-daily-updater |  | True |  |  |
| False | False | justhodl-ab-test |  | True |  |  |
| False | True | justhodl-activist-filings-scanner |  | True |  |  |
| False | False | justhodl-allocator |  | True |  |  |
| False | True | justhodl-asymmetric-hunter |  | True |  |  |
| False | False | justhodl-auction-interpreter |  | True |  |  |
| False | False | justhodl-bloomberg-v8 |  | True |  |  |
| False | False | justhodl-bond-regime-detector |  | True |  |  |
| False | False | justhodl-calls-backtest |  | True |  |  |
| False | False | justhodl-correlation-breaks |  | True |  |  |
| False | False | justhodl-correlation-surface |  | True |  |  |
| False | False | justhodl-data-collector |  | True |  |  |
| False | True | justhodl-deep-value-screener |  | True |  |  |
| False | False | justhodl-dep-graph |  | True |  |  |
| False | False | justhodl-divergence-scanner |  | True |  |  |
| False | True | justhodl-earnings-pead |  | True |  |  |
| False | False | justhodl-earnings-whisper |  | True |  |  |
| False | False | justhodl-email-reports-v2 |  | True |  |  |
| False | True | justhodl-eps-revision-velocity |  | True |  |  |
| False | False | justhodl-fed-speak |  | True |  |  |
| False | False | justhodl-health-monitor |  | True |  |  |
| False | True | justhodl-khalid-metrics |  | True |  |  |
| False | False | justhodl-market-interpreter |  | True |  |  |
| False | True | justhodl-microcap-float-squeeze |  | True |  |  |
| False | False | justhodl-ml-predictions |  | True |  |  |
| False | True | justhodl-momentum-breakout |  | True |  |  |
| False | False | justhodl-momentum-scanner |  | True |  |  |
| False | True | justhodl-morning-brief-tg |  | True |  |  |
| False | False | justhodl-narrative-density-tracker |  | True |  |  |
| False | True | justhodl-nobrainer-rationale |  | True |  |  |
| False | True | justhodl-nobrainer-tracker |  | True |  |  |
| False | True | justhodl-options-flow-scanner |  | True |  |  |
| False | True | justhodl-pead-detector |  | True |  |  |
| False | False | justhodl-position-monitor |  | True |  |  |
| False | False | justhodl-position-sizer-v2 |  | True |  |  |
| False | True | justhodl-pre-pump-detector |  | True |  |  |
| False | False | justhodl-regime-composite |  | True |  |  |
| False | False | justhodl-reports-builder |  | True |  |  |
| False | True | justhodl-revenue-acceleration |  | True |  |  |
| False | False | justhodl-risk-sizer |  | True |  |  |
| False | True | justhodl-sector-earnings-diffusion |  | True |  |  |
| False | False | justhodl-sector-tilt |  | True |  |  |
| False | False | justhodl-signal-portfolio |  | True |  |  |
| False | False | justhodl-smart-money-cluster |  | True |  |  |
| False | True | justhodl-supply-inflection-scanner |  | True |  |  |
| False | False | justhodl-synthetic-monitor |  | True |  |  |
| False | True | justhodl-tape-reader |  | True |  |  |
| False | True | justhodl-theme-detector |  | True |  |  |
| False | False | justhodl-theme-rotation-engine |  | True |  |  |
| False | True | justhodl-theme-tier-classifier |  | True |  |  |
| False | False | justhodl-universe-builder |  | True |  |  |
| False | False | justhodl-vol-regime |  | True |  |  |
| False | True | justhodl-volatility-squeeze-hunter |  | True |  |  |
| False | False | justhodl-watchlist-debate |  | True |  |  |
| False | False | justhodl-whats-changed |  | True |  |  |
| False | False | ofrapi |  | True |  |  |
| False | False | permanent-market-intelligence |  | True |  |  |
| False | False | scrapeMacroData |  | True |  |  |
| False | False | treasury-auto-updater |  | True |  |  |
| False | False | ultimate-multi-agent |  | False |  |  |

## Log
## A. proof

- `01:17:55` router log (2d, 0 lines): []
- `01:17:55` legacy key config/schedule-manifest.json: keys=['version', 'generated_at', 'source', 'doctrine', 'rules', 'schedules'] has_ticks=False source=ops_4237 snapshot of live AWS after the ops 4229-4232 cleanup
- `01:18:45` tick rules -> router: [{"rule": "jhk-tick-15min", "state": "ENABLED", "expr": "cron(53 17 * * ? *)", "tick": "15min"}, {"rule": "jhk-tick-30min", "state": "ENABLED", "expr": "cron(55 14 * * ? *)", "tick": "30min"}, {"rule": "jhk-tick-4hourly", "state": "ENABLED", "expr": "cron(6 18 * * ? *)", "tick": "4hourly"}, {"rule": "jhk-tick-5min", "state": "ENABLED", "expr": "cron(24 21 * * ? *)", "tick": "5min"}, {"rule": "jhk-tick-daily-06utc", "state": "ENABLED", "expr": "cron(0 6 * * ? *)", "tick": "daily-06utc"}, {"rule": "jhk-tick-daily-07utc", "state": "ENABLED", "expr": "cron(0 7 * * ? *)", "tick": "daily-07utc"}, {"rule": "jhk-tick-daily-08utc", "state": "ENABLED", "expr": "cron(0 8 * * ? *)", "tick": "daily-08utc"}, {"rule": "jhk-tick-daily-15utc", "state": "ENABLED", "expr": "cron(0 15 * * ? *)", "tick": "daily-15utc"}, {"rule": "jhk-tick-daily-16utc", "state": "ENABLED", "expr": "cron(0 16 * * ? *)", "tick": "daily-16utc"}, {"rule": "jhk-tick-daily-17utc", "state": "ENABLED", "expr": "cron(0 17 * * ? *)", "tick": "daily-17utc"}, {"rule": "jhk-tick-daily-eve", "state": "ENABLED", "expr": "cron(0 22 * * ? *)", "tick": "daily-eve"}, {"rule": "jhk-tick-daily-morn", "state": "ENABLED", "expr": "cron(0 11 * * ? *)", "tick": "daily-morn"}, {"rule": "jhk-tick-every_3h", "state": "ENABLED", "expr": "cron(17 19 * * ? *)", "tick": "every_3h"}, {"rule": "jhk-tick-every_6h", "state": "ENABLED", "expr": "cron(22 16 * * ? *)", "tick": "every_6h"}, {"rule": "jhk-tick-hourly", "state": "ENABLED", "expr": "cron(5
- `01:18:45` tick -> UTC hours: {"15min": [17], "30min": [14], "4hourly": [18], "5min": [21], "daily-06utc": [6], "daily-07utc": [7], "daily-08utc": [8], "daily-15utc": [15], "daily-16utc": [16], "daily-17utc": [17], "daily-eve": [22], "daily-morn": [11], "every_3h": [19], "every_6h": [16], "hourly": [17], "monthly": [12], "weekly-sun": [12]}
- `01:18:46` router invocations last 7d (daily): [16, 15, 16, 15, 15, 15, 15]
## B. membership + tick assignment

- `01:18:46` 63 fan-out members by signature: MLPredictor, bls-employment-api-v2, daily-liquidity-report, ecb-data-daily-updater, justhodl-ab-test, justhodl-activist-filings-scanner, justhodl-allocator, justhodl-asymmetric-hunter, justhodl-auction-interpreter, justhodl-bloomberg-v8, justhodl-bond-regime-detector, justhodl-calls-backtest, justhodl-correlation-breaks, justhodl-correlation-surface, justhodl-data-collector, justhodl-deep-value-screener, justhodl-dep-graph, justhodl-divergence-scanner, justhodl-earnings-pead, justhodl-earnings-whisper, justhodl-email-reports-v2, justhodl-eps-revision-velocity, justhodl-fed-speak, justhodl-health-monitor, justhodl-khalid-metrics, justhodl-market-interpreter, justhodl-microcap-float-squeeze, justhodl-ml-predictions, justhodl-momentum-breakout, justhodl-momentum-scanner, justhodl-morning-brief-tg, justhodl-narrative-density-tracker, justhodl-nobrainer-rationale, justhodl-nobrainer-tracker, justhodl-options-flow-scanner, justhodl-pead-detector, justhodl-position-monitor, justhodl-position-sizer-v2, justhodl-pre-pump-detector, justhodl-regime-composite, justhodl-reports-builder, justhodl-revenue-acceleration, justhodl-risk-sizer, justhodl-sector-earnings-diffusion, justhodl-sector-tilt, justhodl-signal-portfolio, justhodl-smart-money-cluster, justhodl-supply-inflection-scanner, justhodl-synthetic-monitor, justhodl-tape-reader, justhodl-theme-detector, justhodl-theme-rotation-engine, justhodl-theme-tier-classifier, justhodl-universe-builder, justhodl-vol-regime, justhodl-volatility-squeeze-hunter, justhodl-watchlist-debate, justhodl-whats-changed, ofrapi, permanent-market-intelligence, scrapeMacroData, treasury-auto-updater, ultimate-multi-agent
- `01:18:46`   MLPredictor                                  hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   bls-employment-api-v2                        hours=[22] per_day=1.0 -> ['daily-eve']
- `01:18:46`   daily-liquidity-report                       hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   ecb-data-daily-updater                       hours=[6] per_day=1.0 -> ['daily-06utc']
- `01:18:46`   justhodl-ab-test                             hours=[16] per_day=1.0 -> ['daily-16utc', 'every_6h']
- `01:18:46`   justhodl-activist-filings-scanner            hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-allocator                           hours=[18] per_day=1.0 -> ['4hourly']
- `01:18:46`   justhodl-asymmetric-hunter                   hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-auction-interpreter                 hours=[18] per_day=1.0 -> ['4hourly']
- `01:18:46`   justhodl-bloomberg-v8                        hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   justhodl-bond-regime-detector                hours=[18] per_day=1.0 -> ['4hourly']
- `01:18:46`   justhodl-calls-backtest                      hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-correlation-breaks                  hours=[22] per_day=1.0 -> ['daily-eve']
- `01:18:46`   justhodl-correlation-surface                 hours=[15] per_day=1.0 -> ['daily-15utc']
- `01:18:46`   justhodl-data-collector                      hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   justhodl-deep-value-screener                 hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-dep-graph                           hours=[22] per_day=1.0 -> ['daily-eve']
- `01:18:46`   justhodl-divergence-scanner                  hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-earnings-pead                       hours=[8] per_day=1.0 -> ['daily-08utc']
- `01:18:46`   justhodl-earnings-whisper                    hours=[8] per_day=1.0 -> ['daily-08utc']
- `01:18:46`   justhodl-email-reports-v2                    hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-eps-revision-velocity               hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-fed-speak                           hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-health-monitor                      hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   justhodl-khalid-metrics                      hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-market-interpreter                  hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   justhodl-microcap-float-squeeze              hours=[22] per_day=1.0 -> ['daily-eve']
- `01:18:46`   justhodl-ml-predictions                      hours=[18] per_day=1.0 -> ['4hourly']
- `01:18:46`   justhodl-momentum-breakout                   hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-momentum-scanner                    hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-morning-brief-tg                    hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-narrative-density-tracker           hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-nobrainer-rationale                 hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-nobrainer-tracker                   hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   justhodl-options-flow-scanner                hours=[22] per_day=1.0 -> ['daily-eve']
- `01:18:46`   justhodl-pead-detector                       hours=[8] per_day=1.0 -> ['daily-08utc']
- `01:18:46`   justhodl-position-monitor                    hours=[14] per_day=1.0 -> ['30min']
- `01:18:46`   justhodl-position-sizer-v2                   hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-pre-pump-detector                   hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-regime-composite                    hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   justhodl-reports-builder                     hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   justhodl-revenue-acceleration                hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-risk-sizer                          hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-sector-earnings-diffusion           hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-sector-tilt                         hours=[18] per_day=1.0 -> ['4hourly']
- `01:18:46`   justhodl-signal-portfolio                    hours=[22] per_day=1.0 -> ['daily-eve']
- `01:18:46`   justhodl-smart-money-cluster                 hours=[16] per_day=1.0 -> ['daily-16utc', 'every_6h']
- `01:18:46`   justhodl-supply-inflection-scanner           hours=[7] per_day=1.0 -> ['daily-07utc']
- `01:18:46`   justhodl-synthetic-monitor                   hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   justhodl-tape-reader                         hours=[22] per_day=1.0 -> ['daily-eve']
- `01:18:46`   justhodl-theme-detector                      hours=[6] per_day=1.0 -> ['daily-06utc']
- `01:18:46`   justhodl-theme-rotation-engine               hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-theme-tier-classifier               hours=[8] per_day=1.0 -> ['daily-08utc']
- `01:18:46`   justhodl-universe-builder                    hours=[18] per_day=1.06 -> ['4hourly']
- `01:18:46`   justhodl-vol-regime                          hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   justhodl-volatility-squeeze-hunter           hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   justhodl-watchlist-debate                    hours=[22] per_day=1.0 -> ['daily-eve']
- `01:18:46`   justhodl-whats-changed                       hours=[17] per_day=1.0 -> ['15min', 'daily-17utc', 'hourly']
- `01:18:46`   ofrapi                                       hours=[11] per_day=1.88 -> ['daily-morn']
- `01:18:46`   permanent-market-intelligence                hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   scrapeMacroData                              hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   treasury-auto-updater                        hours=[11] per_day=1.0 -> ['daily-morn']
- `01:18:46`   ultimate-multi-agent                         hours=[] per_day=3.69 -> ['daily-morn'] (default)
- `01:18:46` ticks: {"daily-morn": 28, "daily-eve": 8, "daily-06utc": 2, "daily-16utc": 2, "every_6h": 2, "4hourly": 6, "15min": 10, "daily-17utc": 10, "hourly": 10, "daily-15utc": 1, "daily-08utc": 4, "30min": 1, "daily-07utc": 1}
## C. write manifest + redeploy router

- `01:18:47` ✅ manifest written to s3://justhodl-dashboard-live/config/fanout-manifest.json and config/fanout-manifest.json
- `01:18:47`   zip: 100354 bytes
## 1. Lambda

- `01:18:47`   Lambda exists — updating
- `01:18:53` ✅   ✓ updated justhodl-scheduler
## D. fire + verify

- `01:19:01` router tick 15min: {"statusCode": 200, "body": "{\"tick\": \"15min\", \"invoked_ok\": 10, \"invoked_err\": 0, \"elapsed_s\": 0.69, \"errors\": []}"}
- `01:19:01` router tick 30min: {"statusCode": 200, "body": "{\"tick\": \"30min\", \"invoked_ok\": 1, \"invoked_err\": 0, \"elapsed_s\": 0.1, \"errors\": []}"}
- `01:19:01` router tick 4hourly: {"statusCode": 200, "body": "{\"tick\": \"4hourly\", \"invoked_ok\": 6, \"invoked_err\": 0, \"elapsed_s\": 0.31, \"errors\": []}"}
- `01:19:02` router tick daily-06utc: {"statusCode": 200, "body": "{\"tick\": \"daily-06utc\", \"invoked_ok\": 2, \"invoked_err\": 0, \"elapsed_s\": 0.19, \"errors\": []}"}
- `01:19:02` router tick daily-07utc: {"statusCode": 200, "body": "{\"tick\": \"daily-07utc\", \"invoked_ok\": 1, \"invoked_err\": 0, \"elapsed_s\": 0.11, \"errors\": []}"}
- `01:19:02` router tick daily-08utc: {"statusCode": 200, "body": "{\"tick\": \"daily-08utc\", \"invoked_ok\": 4, \"invoked_err\": 0, \"elapsed_s\": 0.28, \"errors\": []}"}
- `01:19:03` router tick daily-15utc: {"statusCode": 200, "body": "{\"tick\": \"daily-15utc\", \"invoked_ok\": 1, \"invoked_err\": 0, \"elapsed_s\": 0.09, \"errors\": []}"}
- `01:19:03` router tick daily-16utc: {"statusCode": 200, "body": "{\"tick\": \"daily-16utc\", \"invoked_ok\": 2, \"invoked_err\": 0, \"elapsed_s\": 0.15, \"errors\": []}"}
- `01:19:03` router tick daily-17utc: {"statusCode": 200, "body": "{\"tick\": \"daily-17utc\", \"invoked_ok\": 10, \"invoked_err\": 0, \"elapsed_s\": 0.51, \"errors\": []}"}
- `01:19:04` router tick daily-eve: {"statusCode": 200, "body": "{\"tick\": \"daily-eve\", \"invoked_ok\": 8, \"invoked_err\": 0, \"elapsed_s\": 0.43, \"errors\": []}"}
- `01:19:05` router tick daily-morn: {"statusCode": 200, "body": "{\"tick\": \"daily-morn\", \"invoked_ok\": 28, \"invoked_err\": 0, \"elapsed_s\": 1.46, \"errors\": []}"}
- `01:19:06` router tick every_6h: {"statusCode": 200, "body": "{\"tick\": \"every_6h\", \"invoked_ok\": 2, \"invoked_err\": 0, \"elapsed_s\": 0.13, \"errors\": []}"}
- `01:19:06` router tick hourly: {"statusCode": 200, "body": "{\"tick\": \"hourly\", \"invoked_ok\": 10, \"invoked_err\": 0, \"elapsed_s\": 0.53, \"errors\": []}"}
- `01:26:10` invoked 62/63: MLPredictor, bls-employment-api-v2, daily-liquidity-report, ecb-data-daily-updater, justhodl-ab-test, justhodl-activist-filings-scanner, justhodl-allocator, justhodl-asymmetric-hunter, justhodl-auction-interpreter, justhodl-bloomberg-v8, justhodl-bond-regime-detector, justhodl-calls-backtest, justhodl-correlation-breaks, justhodl-correlation-surface, justhodl-data-collector, justhodl-deep-value-screener, justhodl-dep-graph, justhodl-divergence-scanner, justhodl-earnings-pead, justhodl-earnings-whisper, justhodl-email-reports-v2, justhodl-eps-revision-velocity, justhodl-fed-speak, justhodl-health-monitor, justhodl-khalid-metrics, justhodl-market-interpreter, justhodl-microcap-float-squeeze, justhodl-ml-predictions, justhodl-momentum-breakout, justhodl-momentum-scanner, justhodl-morning-brief-tg, justhodl-narrative-density-tracker, justhodl-nobrainer-rationale, justhodl-nobrainer-tracker, justhodl-options-flow-scanner, justhodl-pead-detector, justhodl-position-monitor, justhodl-position-sizer-v2, justhodl-pre-pump-detector, justhodl-regime-composite, justhodl-reports-builder, justhodl-revenue-acceleration, justhodl-risk-sizer, justhodl-sector-earnings-diffusion, justhodl-sector-tilt, justhodl-signal-portfolio, justhodl-smart-money-cluster, justhodl-supply-inflection-scanner, justhodl-synthetic-monitor, justhodl-tape-reader, justhodl-theme-detector, justhodl-theme-rotation-engine, justhodl-theme-tier-classifier, justhodl-universe-builder, justhodl-vol-regime, justhodl-volatility-squeeze-hunter, justhodl-watchlist-debate, justhodl-whats-changed, ofrapi, permanent-market-intelligence, scrapeMacroData, treasury-auto-updater
- `01:26:10` errored 0: 
- `01:26:10` feeds advanced (24): [["justhodl-activist-filings-scanner", "data/universe.json"], ["justhodl-asymmetric-hunter", "data/nobrainers.json"], ["justhodl-deep-value-screener", "data/universe.json"], ["justhodl-earnings-pead", "data/universe.json"], ["justhodl-eps-revision-velocity", "data/universe.json"], ["justhodl-khalid-metrics", "data/khalid-metrics.json"], ["justhodl-microcap-float-squeeze", "data/universe.json"], ["justhodl-momentum-breakout", "data/universe.json"], ["justhodl-morning-brief-tg", "data/morning-brief-latest.json"], ["justhodl-nobrainer-rationale", "data/deep-value.json"], ["justhodl-nobrainer-rationale", "data/eps-revision-velocity.json"], ["justhodl-nobrainer-tracker", "data/nobrainers.json"], ["justhodl-options-flow-scanner", "data/universe.json"], ["justhodl-pead-detector", "data/universe.json"], ["justhodl-pre-pump-detector", "data/universe.json"], ["justhodl-revenue-acceleration", "data/universe.json"], ["justhodl-sector-earnings-diffusion", "data/eps-revision-velocity.json"], ["justhodl-sector-earnings-diffusion", "data/universe.json"], ["justhodl-supply-inflection-scanner", "data/supply-inflection.json"], ["justhodl-tape-reader", "data/universe.json"], ["justhodl-theme-detector", "data/themes-detected.json"], ["justhodl-theme-tier-classifier", "data/theme-tiers.json"], ["justhodl-theme-tier-classifier", "data/themes-detected.json"], ["justhodl-volatility-squeeze-hunter", "data/universe.json"]]
- `01:26:10` not invoked (1): ultimate-multi-agent
## verdict

- `01:26:10` ✅ VERDICT: GREEN -- fan-out restored: 63 members across ticks {'daily-morn': 28, 'daily-eve': 8, 'daily-06utc': 2, 'daily-16utc': 2, 'every_6h': 2, '4hourly': 6, '15min': 10, 'daily-17utc': 10, 'hourly': 10, 'daily-15utc': 1, 'daily-08utc': 4, '30min': 1, 'daily-07utc': 1}; 62 invoked now, 0 errored (-> FIX_ERRORS wave), 24 feeds advanced
