# Phase 2b — trace callers of 11 preserved Lambdas

**Status:** success  
**Duration:** 25.1s  
**Finished:** 2026-04-22T23:49:56+00:00  

## Data

| api_gw | code_callers | eb_rules | has_url | inv_90d | target |
|---|---|---|---|---|---|
| 0 | — | 2 | False | 94802 | enhanced-openbb-handler |
| 0 | justhodl-daily-report-v3, justhodl-khalid-metrics, scrapeMacroData | 0 | False | 9774 | ecb |
| 0 | — | 1 | False | 3521 | justhodl-data-collector |
| 0 | — | 1 | False | 270 | ecb-data-daily-updater |
| 0 | — | 2 | False | 106 | ofrapi |
| 0 | — | 2 | False | 90 | justhodl-email-reports-v2 |
| 0 | — | 1 | False | 44 | justhodl-liquidity-agent |
| 0 | — | 1 | False | 13 | ecb-auto-updater |
| 0 | — | 1 | False | 6 | justhodl-calibrator |
| 0 | — | 0 | False | 1 | nyfed-cmdi-fetcher |
| 0 | — | 0 | False | 1 | nyfed-main-aggregator |

## Log
- `23:49:31` Started at 2026-04-22T23:49:31.689998+00:00
## Step 1: resource policies + function URLs + EB rules

- `23:49:32`   enhanced-openbb-handler: EB=2 | policy=apigateway.amazonaws.com,events.amazonaws.com
- `23:49:32`   ecb: policy=apigateway.amazonaws.com
- `23:49:32`   justhodl-data-collector: EB=1 | policy=events.amazonaws.com
- `23:49:33`   ecb-data-daily-updater: EB=1 | policy=events.amazonaws.com
- `23:49:33`   ofrapi: EB=2 | policy=apigateway.amazonaws.com,events.amazonaws.com
- `23:49:33`   justhodl-email-reports-v2: EB=2 | policy=events.amazonaws.com
- `23:49:34`   justhodl-liquidity-agent: EB=1 | policy=events.amazonaws.com
- `23:49:34`   ecb-auto-updater: EB=1 | policy=events.amazonaws.com
- `23:49:34`   justhodl-calibrator: EB=1 | policy=events.amazonaws.com
- `23:49:35`   nyfed-cmdi-fetcher: policy=apigateway.amazonaws.com
- `23:49:35`   nyfed-main-aggregator: policy=apigateway.amazonaws.com
## Step 2: API Gateway integrations (REST + HTTP)

- `23:49:35`   (no API Gateway integrations target any preserved Lambda)
## Step 3: static analysis — scan 98 Lambdas' code for callers

- `23:49:36`   Scanning 98 zip-packaged Lambdas…
- `23:49:38`     scanned 10/98…
- `23:49:40`     scanned 20/98…
- `23:49:43`     scanned 30/98…
- `23:49:45`     scanned 40/98…
- `23:49:47`     scanned 50/98…
- `23:49:50`     scanned 60/98…
- `23:49:52`     scanned 70/98…
- `23:49:55`     scanned 80/98…
- `23:49:56`   Done scanning 87 Lambdas
## Step 4: caller map per preserved Lambda

- `23:49:56` 
- `23:49:56` ### enhanced-openbb-handler (94802 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
- `23:49:56`   EB RULES: lambda-warmer-system3-frequent, lambda-warmer-system3
- `23:49:56` 
- `23:49:56` ### ecb (9774 invocations/90d)
- `23:49:56`   CODE CALLERS (3): justhodl-daily-report-v3, justhodl-khalid-metrics, scrapeMacroData
- `23:49:56`     ← justhodl-daily-report-v3  lambda_function.py:141: 'ECBASSETSW':('ecb','ECB Total Assets'), 'ECBDFR':('ecb','ECB Deposit Rate'),
- `23:49:56`     ← justhodl-daily-report-v3  lambda_function.py:142: 'ECBMLFR':('ecb','ECB Main Refi Rate'), 'INTDSREZM193N':('ecb','Euro Deposit Rate'),
- `23:49:56`     ← justhodl-daily-report-v3  lambda_function.py:143: 'CLVMNACSCAB1GQEA19':('ecb','Euro Real GDP'), 'EA19CPALTT01GYM':('ecb','Euro CPI YoY'),
- `23:49:56`     ← justhodl-daily-report-v3  lambda_function.py:144: 'LRHUTTTTEZM156S':('ecb','Euro Unemployment'), 'IR3TIB01EZM156N':('ecb','Euro 3M Interbank'),
- `23:49:56`     ← justhodl-daily-report-v3  lambda_function.py:145: 'IRLTLT01EZM156N':('ecb','Euro LT Govt Bond'), 'CP0000EZ19M086NEST':('ecb','Euro HICP'),
- `23:49:56`     …and 12 more ref(s)
- `23:49:56` 
- `23:49:56` ### justhodl-data-collector (3521 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
- `23:49:56`   EB RULES: justhodl-hourly-collection
- `23:49:56` 
- `23:49:56` ### ecb-data-daily-updater (270 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
- `23:49:56`   EB RULES: ecb-daily-update-rule
- `23:49:56` 
- `23:49:56` ### ofrapi (106 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
- `23:49:56`   EB RULES: ofr-daily-collection, ofr-weekly-report
- `23:49:56` 
- `23:49:56` ### justhodl-email-reports-v2 (90 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
- `23:49:56`   EB RULES: DailyEmailReportsV2, DailyEmailReportsV2_8AMET
- `23:49:56` 
- `23:49:56` ### justhodl-liquidity-agent (44 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
- `23:49:56`   EB RULES: justhodl-liquidity-agent-daily
- `23:49:56` 
- `23:49:56` ### ecb-auto-updater (13 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
- `23:49:56`   EB RULES: ecb-weekly-update
- `23:49:56` 
- `23:49:56` ### justhodl-calibrator (6 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
- `23:49:56`   EB RULES: justhodl-calibrator-weekly
- `23:49:56` 
- `23:49:56` ### nyfed-cmdi-fetcher (1 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
- `23:49:56` 
- `23:49:56` ### nyfed-main-aggregator (1 invocations/90d)
- `23:49:56`   CODE CALLERS: (none found)
## Step 5: verdict

- `23:49:56`   enhanced-openbb-handler: KEEP — scheduled by EB: lambda-warmer-system3-frequent
- `23:49:56`   ecb: KEEP — invoked by: justhodl-daily-report-v3, justhodl-khalid-metrics, scrapeMacroData
- `23:49:56`   justhodl-data-collector: KEEP — scheduled by EB: justhodl-hourly-collection
- `23:49:56`   ecb-data-daily-updater: KEEP — scheduled by EB: ecb-daily-update-rule
- `23:49:56`   ofrapi: KEEP — scheduled by EB: ofr-daily-collection
- `23:49:56`   justhodl-email-reports-v2: KEEP — scheduled by EB: DailyEmailReportsV2
- `23:49:56`   justhodl-liquidity-agent: KEEP — scheduled by EB: justhodl-liquidity-agent-daily
- `23:49:56`   ecb-auto-updater: KEEP — scheduled by EB: ecb-weekly-update
- `23:49:56`   justhodl-calibrator: KEEP — scheduled by EB: justhodl-calibrator-weekly
- `23:49:56`   nyfed-cmdi-fetcher: SAFE TO DELETE — no callers found yet still getting invocations. CloudTrail needed to be 100% sure.
- `23:49:56`   nyfed-main-aggregator: SAFE TO DELETE — no callers found yet still getting invocations. CloudTrail needed to be 100% sure.
- `23:49:56` Done
