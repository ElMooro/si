# ops 6717 - trigger cap: the two role grants + 60-day invocation peaks for the budgets

**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-11-03T03:45:31+00:00  

## Data

| engines |
|---|
| 49 |

## Log
## A. IAM grants

- `03:45:28` ✅ justhodl-spend-governor-role += trigger-ledger-governor: {'dynamodb:UpdateItem': 'allowed', 'dynamodb:GetItem': 'allowed', 'dynamodb:Scan': 'allowed'}
- `03:45:29` ✅ justhodl-student-rsi-role += trigger-ledger-engine: {'dynamodb:UpdateItem': 'allowed', 'dynamodb:GetItem': 'allowed', 'lambda:InvokeFunction': 'allowed'}
## B. invocations per day, last 60 days (CloudWatch Sum, period 1 day)

- `03:45:31` justhodl-symdir                      active days 56 | peak  45951 | p95  21131 | mean   4264.8 | storm [('2026-10-12', 7196), ('2026-10-13', 6969)]
- `03:45:31` justhodl-tv-bars                     active days 56 | peak   9221 | p95   6023 | mean    470.2 | storm [('2026-10-12', 1333), ('2026-10-13', 1215)]
- `03:45:31` justhodl-equity-research             active days 56 | peak   5321 | p95   2263 | mean   1236.0 | storm [('2026-10-12', 1122), ('2026-10-13', 423)]
- `03:45:31` justhodl-fundamental-graphs          active days 56 | peak   3885 | p95   2858 | mean    577.3 | storm [('2026-10-12', 1053), ('2026-10-13', 110)]
- `03:45:31` justhodl-sdmx-walker                 active days 56 | peak   1827 | p95   1826 | mean   1361.7 | storm [('2026-10-12', 909), ('2026-10-13', 705)]
- `03:45:31` justhodl-student-rsi                 active days 35 | peak   1441 | p95   1440 | mean   1382.1 | storm [('2026-10-12', 961), ('2026-10-13', 859)]
- `03:45:31` justhodl-boj-full                    active days 56 | peak   1105 | p95   1105 | mean    828.5 | storm [('2026-10-12', 526), ('2026-10-13', 401)]
- `03:45:31` justhodl-a2a-bus                     active days 54 | peak    994 | p95    994 | mean    741.4 | storm [('2026-10-12', 661), ('2026-10-13', 551)]
- `03:45:31` justhodl-research-critique           active days 51 | peak    690 | p95    605 | mean    190.5 | storm [('2026-10-12', 29), ('2026-10-15', 28)]
- `03:45:31` justhodl-streaming-fanout            active days 40 | peak    481 | p95    480 | mean    356.9 | storm [('2026-10-12', 392), ('2026-10-13', 309)]
- `03:45:31` justhodl-gdelt-full                  active days 55 | peak    292 | p95    292 | mean    222.2 | storm [('2026-10-12', 204), ('2026-10-13', 169)]
- `03:45:31` justhodl-census-us                   active days 54 | peak    216 | p95    211 | mean    164.7 | storm [('2026-10-12', 210), ('2026-10-13', 210)]
- `03:45:31` justhodl-fred-catalog                active days 55 | peak    192 | p95    192 | mean    146.4 | storm [('2026-10-12', 192), ('2026-10-13', 192)]
- `03:45:31` justhodl-import-sentinel             active days 55 | peak    145 | p95    145 | mean    109.8 | storm [('2026-10-12', 97), ('2026-10-13', 86)]
- `03:45:31` treasury-api                         active days 44 | peak    108 | p95     99 | mean     43.7 | storm [('2026-10-12', 25), ('2026-10-13', 23)]
- `03:45:31` justhodl-backend-agent               active days 54 | peak     96 | p95     96 | mean     74.7 | storm [('2026-10-12', 66), ('2026-10-13', 51)]
- `03:45:31` openbb-websocket-broadcast           active days 55 | peak     71 | p95     68 | mean     26.6 | storm [('2026-10-12', 7), ('2026-10-13', 5)]
- `03:45:31` justhodl-worldbank-full              active days 55 | peak     66 | p95     59 | mean     16.7 | storm [('2026-10-12', 47), ('2026-10-13', 20)]
- `03:45:31` justhodl-alpha-compass               active days 55 | peak     57 | p95     47 | mean     25.8 | storm [('2026-10-12', 8), ('2026-10-13', 8)]
- `03:45:31` justhodl-portfolio-snapshot          active days 55 | peak     51 | p95     49 | mean     36.0 | storm [('2026-10-12', 35), ('2026-10-13', 33)]
- `03:45:31` justhodl-sec-13f                     active days 56 | peak     49 | p95     48 | mean     36.0 | storm [('2026-10-12', 48), ('2026-10-13', 48)]
- `03:45:31` justhodl-jh-fusion                   active days 51 | peak     29 | p95     26 | mean     19.2 | storm [('2026-10-12', 9), ('2026-10-13', 3)]
- `03:45:31` justhodl-13f-positions               active days 55 | peak     26 | p95     24 | mean     15.2 | storm [('2026-10-12', 24), ('2026-10-13', 24)]
- `03:45:31` justhodl-miss-calibrator             active days 10 | peak     24 | p95     24 | mean     16.2 | storm [('2026-10-12', 2323091), ('2026-10-13', 2610077)]
- `03:45:31` justhodl-audit-loop                  active days 54 | peak     12 | p95     12 | mean      9.5 | storm [('2026-10-12', 7), ('2026-10-13', 10)]
- `03:45:31` justhodl-factory-grader              active days  4 | peak     12 | p95     12 | mean      9.5 | storm []
- `03:45:31` justhodl-stock-screener              active days 54 | peak      9 | p95      8 | mean      5.6 | storm [('2026-10-12', 5), ('2026-10-13', 5)]
- `03:45:31` justhodl-outcome-checker             active days 55 | peak      8 | p95      6 | mean      4.0 | storm [('2026-10-12', 2), ('2026-10-13', 2)]
- `03:45:31` justhodl-signal-board                active days 55 | peak      7 | p95      7 | mean      3.6 | storm [('2026-10-12', 3), ('2026-10-13', 2)]
- `03:45:31` justhodl-trend-reversal              active days 40 | peak      7 | p95      7 | mean      6.3 | storm [('2026-10-12', 1), ('2026-10-13', 5)]
- `03:45:31` justhodl-ecb-deep                    active days 54 | peak      6 | p95      5 | mean      3.4 | storm [('2026-10-12', 3), ('2026-10-13', 2)]
- `03:45:31` justhodl-engine-signal-map           active days 53 | peak      6 | p95      4 | mean      1.3 | storm [('2026-10-12', 1), ('2026-10-14', 1)]
- `03:45:31` justhodl-fiscaldata-full             active days 51 | peak      6 | p95      3 | mean      1.4 | storm [('2026-10-14', 1)]
- `03:45:31` justhodl-imf-full                    active days  8 | peak      6 | p95      6 | mean      3.7 | storm [('2026-10-12', 3)]
- `03:45:31` justhodl-alpha-calibrator            active days 20 | peak      5 | p95      5 | mean      1.8 | storm [('2026-10-15', 2), ('2026-10-16', 2)]
- `03:45:31` justhodl-calibrator                  active days 27 | peak      5 | p95      4 | mean      1.5 | storm [('2026-10-15', 2), ('2026-10-16', 1)]
- `03:45:31` justhodl-fundamental-census          active days  2 | peak      5 | p95      5 | mean      5.0 | storm []
- `03:45:31` justhodl-screener-alerts             active days 52 | peak      5 | p95      5 | mean      2.9 | storm [('2026-10-12', 2), ('2026-10-13', 2)]
- `03:45:31` justhodl-13f-clone-alpha             active days  7 | peak      2 | p95      2 | mean      2.0 | storm [('2026-10-12', 2)]
- `03:45:31` justhodl-finra-full                  active days 50 | peak      2 | p95      2 | mean      1.1 | storm []
- `03:45:31` justhodl-bls-full                    active days 50 | peak      1 | p95      1 | mean      1.0 | storm []
- `03:45:31` justhodl-bottleneck-boom             active days 53 | peak      1 | p95      1 | mean      1.0 | storm [('2026-10-12', 1), ('2026-10-13', 1)]
- `03:45:31` justhodl-bottleneck-research         active days 52 | peak      1 | p95      1 | mean      1.0 | storm [('2026-10-13', 1), ('2026-10-14', 1)]
- `03:45:31` justhodl-equity-prewarm              active days 52 | peak      1 | p95      1 | mean      1.0 | storm [('2026-10-12', 1), ('2026-10-15', 1)]
- `03:45:31` justhodl-hist-banker                 active days  5 | peak      1 | p95      1 | mean      1.0 | storm []
- `03:45:31` justhodl-page-ai-commentary          active days 38 | peak      1 | p95      1 | mean      1.0 | storm [('2026-10-12', 1), ('2026-10-16', 1)]
- `03:45:31` justhodl-polygon-full                active days 52 | peak      1 | p95      1 | mean      1.0 | storm [('2026-10-13', 1), ('2026-10-16', 1)]
- `03:45:31` justhodl-portfolio-admin             active days  1 | peak      1 | p95      1 | mean      1.0 | storm []
- `03:45:31` justhodl-sec-midas                   active days 53 | peak      1 | p95      1 | mean      1.0 | storm [('2026-10-13', 1), ('2026-10-14', 1)]
- `03:45:31` ✅ ops 6717 complete
