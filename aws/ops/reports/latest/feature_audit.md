# System feature audit: requested vs live

**Status:** success  
**Duration:** 10.8s  
**Finished:** 2026-04-25T02:12:39+00:00  

## Data

| green | missing | red | total | yellow |
|---|---|---|---|---|
| 59 | 9 | 2 | 72 | 2 |

## Log
## A. Core Lambdas

- `02:12:29`   🟢 justhodl-daily-report-v3                   290 inv / 0 err 24h
- `02:12:29`   🟢 justhodl-ai-chat                           10 inv / 0 err 24h
- `02:12:30`   🟢 justhodl-bloomberg-v8                      288 inv / 0 err 24h
- `02:12:30`   🟢 justhodl-intelligence                      16 inv / 0 err 24h
- `02:12:30`   🟢 justhodl-morning-intelligence              1 inv / 0 err 24h
- `02:12:30`   🟢 justhodl-edge-engine                       4 inv / 0 err 24h
- `02:12:31`   🟢 justhodl-options-flow                      289 inv / 0 err 24h
- `02:12:31`   🟢 justhodl-investor-agents                   On-demand Lambda; 0 24h invocations is fine
- `02:12:32`   🟢 justhodl-stock-analyzer                    On-demand Lambda; 0 24h invocations is fine
- `02:12:32`   🟢 justhodl-stock-screener                    6 inv / 0 err 24h
- `02:12:32`   🟡 justhodl-valuations-agent                  Has schedule but 0 invocations 24h
- `02:12:32`   🟢 justhodl-crypto-intel                      96 inv / 0 err 24h
- `02:12:33`   🟢 cftc-futures-positioning-agent             308 inv / 0 err 24h
- `02:12:33`   🟢 justhodl-financial-secretary               6 inv / 0 err 24h
- `02:12:34`   🟢 justhodl-repo-monitor                      23 inv / 0 err 24h
- `02:12:34`   🟢 justhodl-dex-scanner                       96 inv / 0 err 24h
- `02:12:34`   🟢 justhodl-telegram-bot                      12 inv / 0 err 24h
- `02:12:35`   🟢 justhodl-signal-logger                     11 inv / 0 err 24h
- `02:12:35`   🟢 justhodl-outcome-checker                   2 inv / 0 err 24h
- `02:12:35`   🟡 justhodl-calibrator                        Has schedule but 0 invocations 24h
- `02:12:36`   🟢 justhodl-health-monitor                    13 inv / 0 err 24h
- `02:12:36`   🟢 justhodl-ml-predictions                    6 inv / 0 err 24h
- `02:12:36`   🟢 justhodl-khalid-metrics                    1 inv / 0 err 24h
- `02:12:37`   🟢 justhodl-advanced-charts                   On-demand Lambda; 0 24h invocations is fine
## B. Dashboard pages

- `02:12:37`   🟢 index.html                     size 55,121B age 1121h
- `02:12:37`   🟢 pro.html                       size 58,557B age 1414h
- `02:12:37`   ⚫ agent.html                     agent.html not in S3 bucket
- `02:12:37`   🟢 charts.html                    size 245,035B age 1437h
- `02:12:37`   🟢 valuations.html                size 25,062B age 1272h
- `02:12:37`   ⚫ edge.html                      edge.html not in S3 bucket
- `02:12:37`   🟢 flow.html                      size 30,349B age 1410h
- `02:12:37`   🟢 intelligence.html              size 27,710B age 1458h
- `02:12:37`   ⚫ risk.html                      risk.html not in S3 bucket
- `02:12:37`   🟢 stocks.html                    size 26,200B age 1289h
- `02:12:37`   🟢 ath.html                       size 15,998B age 1387h
- `02:12:37`   ⚫ trading-signals.html           trading-signals.html not in S3 bucket
- `02:12:37`   ⚫ reports.html                   reports.html not in S3 bucket
- `02:12:37`   ⚫ ml.html                        ml.html not in S3 bucket
- `02:12:37`   🟢 dex.html                       size 49,207B age 1149h
- `02:12:37`   ⚫ liquidity.html                 liquidity.html not in S3 bucket
- `02:12:37`   🟢 health.html                    size 9,996B age 1h
## C. S3 data files

- `02:12:38`   🟢 data/report.json                    Age 0.0h, size 1,724,871B
- `02:12:38`   🟢 crypto-intel.json                   Age 0.0h, size 55,834B
- `02:12:38`   🟢 edge-data.json                      Age 4.1h, size 1,222B
- `02:12:38`   🟢 repo-data.json                      Age 2.7h, size 36,413B
- `02:12:38`   🟢 flow-data.json                      Age 0.1h, size 31,570B
- `02:12:38`   🟢 intelligence-report.json            Age 2.0h, size 4,449B
- `02:12:38`   🟢 screener/data.json                  Age 6.7h, size 326,603B
- `02:12:38`   🟢 valuations-data.json                Age 564.2h, size 2,188B
- `02:12:38`   🟢 calibration/latest.json             Age 137.2h, size 3,899B
- `02:12:38`   🟢 learning/last_log_run.json          Age 1.8h, size 80B
- `02:12:38`   ⚫ dex-scanner-data.json               File missing from S3
- `02:12:38`   🟢 data/secretary-latest.json          Age 0.8h, size 141,252B
- `02:12:38`   ⚫ ath-data.json                       File missing from S3
- `02:12:38`   🔴 predictions.json                    Age 33.3h, expected ≤1h (33.3× over)
- `02:12:38`   🔴 data.json                           Age 1573.2h, expected ≤24h (65.5× over)
## D. SSM parameters

- `02:12:38`   🟢 /justhodl/ai-chat/auth-token                       type SecureString
- `02:12:38`   🟢 /justhodl/calibration/weights                      type String
- `02:12:38`   🟢 /justhodl/calibration/accuracy                     type String
- `02:12:38`   🟢 /justhodl/calibration/report                       type String
- `02:12:38`   🟢 /justhodl/telegram/chat_id                         type String
- `02:12:38`   🟢 /justhodl/telegram/bot_token                       type SecureString
## E. EventBridge rules — critical scheduled events

- `02:12:38`   🟢 justhodl-outcome-checker-daily           ENABLED cron(30 22 ? * MON-FRI *)
- `02:12:38`   🟢 justhodl-outcome-checker-weekly          ENABLED cron(0 8 ? * SUN *)
- `02:12:39`   🟢 justhodl-calibrator-weekly               ENABLED cron(0 9 ? * SUN *)
- `02:12:39`   🟢 justhodl-health-monitor-15min            ENABLED cron(0/15 * * * ? *)
- `02:12:39`   🟢 justhodl-v9-auto-refresh                 ENABLED rate(5 minutes)
- `02:12:39`   🟢 DailyMacroScraper                        DISABLED cron(0 12 * * ? *)
## F. DynamoDB active tables

- `02:12:39`   🟢 justhodl-signals               4,779 items, 2873KB
- `02:12:39`   🟢 justhodl-outcomes              4,307 items, 1701KB
- `02:12:39`   🟢 fed-liquidity-cache            267,828 items, 19389KB
## G. Cloudflare Worker

- `02:12:39`   🟢 justhodl-ai-proxy: source in repo 3256B
## Build audit doc

- `02:12:39` ✅   Wrote: aws/ops/audit/feature_audit_2026-04-25.md (150 lines)
- `02:12:39` Done
