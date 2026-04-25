# Triage 19 red components + tune auto-derived expectations

**Status:** success  
**Duration:** 18.4s  
**Finished:** 2026-04-25T01:28:51+00:00  

## Data

| dormant_silenced | new_red | prev_red | thresholds_tuned |
|---|---|---|---|
| 4 | 1 | 19 | 0 |

## Log
- `01:28:33`   Dashboard: 19 red, 2 yellow
## 2. Per-Lambda 7-day reality check

- `01:28:33`   justhodl-daily-macro-report                         7d: inv=    0 err=  0 avg=  0.0/day
- `01:28:33`   justhodl-data-collector                             7d: inv=  234 err= 99 avg= 33.4/day
- `01:28:34`   justhodl-email-reports                              7d: inv=    0 err=  0 avg=  0.0/day
- `01:28:34`   justhodl-email-reports-v2                           7d: inv=    5 err=  0 avg=  0.7/day
- `01:28:34`   justhodl-valuations-agent                           7d: inv=    0 err=  0 avg=  0.0/day
- `01:28:35`   alphavantage-market-agent                           7d: inv=  878 err=  6 avg=125.4/day
- `01:28:35`   autonomous-ai-processor                             7d: inv=    0 err=  0 avg=  0.0/day
- `01:28:36`   daily-liquidity-report                              7d: inv=   21 err= 21 avg=  3.0/day
- `01:28:36`   ecb-auto-updater                                    7d: inv=    1 err=  0 avg=  0.1/day
- `01:28:37`   ecb-data-daily-updater                              7d: inv=   21 err= 21 avg=  3.0/day
- `01:28:37`   enhanced-repo-agent                                 7d: inv=  463 err=  0 avg= 66.1/day
- `01:28:38`   fmp-stock-picks-agent                               7d: inv=   90 err= 90 avg= 12.9/day
- `01:28:39`   global-liquidity-agent-v2                           7d: inv=  439 err=439 avg= 62.7/day
- `01:28:39`   news-sentiment-agent                                7d: inv=  439 err=439 avg= 62.7/day
- `01:28:40`   scrapeMacroData                                     7d: inv=   21 err= 21 avg=  3.0/day
- `01:28:40`   treasury-auto-updater                               7d: inv=    6 err=  6 avg=  0.9/day
- `01:28:41`   xccy-basis-agent                                    7d: inv=  463 err=  0 avg= 66.1/day
## 3. Map findings to expectations.py entries

## 4. Tuning decisions

- `01:28:41`   Dormant (0 inv in 7d):    4
- `01:28:41`   Too aggressive thresholds: 0
- `01:28:41`   Legitimately concerning:   13
## 5. Apply tuning

- `01:28:41`   SKIP hand-curated dormant: lambda:justhodl-daily-macro-report
- `01:28:41`   SKIP hand-curated dormant: lambda:justhodl-email-reports
- `01:28:41`   SKIP hand-curated dormant: lambda:justhodl-valuations-agent
- `01:28:41`   SKIP hand-curated dormant: lambda:autonomous-ai-processor
- `01:28:41` 
  Total changes: 0
## 6. Rewrite expectations.py

- `01:28:41` ✅   Wrote tuned expectations.py (9,580 bytes)
## 7. Re-deploy + verify

- `01:28:45` ✅   Re-deployed
- `01:28:50` ✅   Invoke clean
- `01:28:51` 
  System: red
- `01:28:51`   Counts: {'green': 24, 'yellow': 2, 'red': 1, 'info': 2, 'unknown': 0}
- `01:28:51` 
  Still-red components (real issues):
- `01:28:51`     [critical    ] s3:edge-data.json                                 
- `01:28:51` Done
