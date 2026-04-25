# AWS cost audit + optimization recommendations

**Status:** failure  
**Duration:** 112.2s  
**Finished:** 2026-04-25T01:39:28+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/92_cost_audit.py", line 244, in <module>
    if total:
       ^^^^^
NameError: name 'total' is not defined. Did you mean: 'today'?

```

## Log
## 1. Total spend by service (last 30 days)

- `01:37:37` ⚠   Cost Explorer fetch failed: An error occurred (AccessDeniedException) when calling the GetCostAndUsage operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: ce:GetCostAndUsage on resource: arn:aws:ce:us-east-1:857687956942:/GetCostAndUsage because no identity-based policy allows the ce:GetCostAndUsage action
## 2. Top 20 Lambdas by GB-seconds (the cost driver)

- `01:39:25`   Total Lambda GB-seconds (30d): 2,202,452
- `01:39:25`   Free tier:                     400,000 GB-s/mo
- `01:39:25`   Over free tier:                1,802,452 GB-s
- `01:39:25` 
- `01:39:25`   name                                     mem  inv-30d  GB-s    avg-ms
- `01:39:25`     justhodl-daily-report-v3                 1024     8762  1600691  182686
- `01:39:25`     scrapeMacroData                          3008       90  237938  900000
- `01:39:25`     justhodl-crypto-intel                    1024     5651  102678   18170
- `01:39:25`     justhodl-options-flow                    1024     8674   89049   10266
- `01:39:25`     cftc-futures-positioning-agent            512     9082   47952   10560
- `01:39:25`     justhodl-bloomberg-v8                    2048     8642   21294    1232
- `01:39:25`     justhodl-ultimate-orchestrator           1024     1935   14564    7527
- `01:39:25`     manufacturing-global-agent                512     1934   11930   12337
- `01:39:25`     dollar-strength-agent                     512     1935   10171   10513
- `01:39:25`     securities-banking-agent                  512     1935    9022    9325
- `01:39:25`     justhodl-repo-monitor                     512      514    7566   29439
- `01:39:25`     fmp-stock-picks-agent                     512      396    6806   34372
- `01:39:25`     justhodl-stock-screener                  1024      182    6482   35616
- `01:39:25`     bond-indices-agent                        512     1959    5644    5762
- `01:39:25`     volatility-monitor-agent                  512     1935    5575    5762
- `01:39:25`     fedliquidityapi                           256     1959    2894    5909
- `01:39:25`     alphavantage-market-agent                 256     3870    2837    2933
- `01:39:25`     justhodl-financial-secretary             1024      185    2771   14980
- `01:39:25`     bls-labor-agent                           256     1935    2462    5089
- `01:39:25`     xccy-basis-agent                          256     1959    1792    3658
## 3. Top 20 CloudWatch Log Groups by stored bytes

- `01:39:25`   Total log storage: 1.56 GB
- `01:39:25`   Free tier: 5 GB
- `01:39:25`   Over: 0.00 GB at $0.03/GB/mo = $0.00
- `01:39:25` 
- `01:39:25`   name                                                bytes      retention
- `01:39:25`     /aws/apprunner/openbb-api/1ccdfbc8a3ab43cca282e6a6fd10a72f/application   803.0MB  FOREVER
- `01:39:25`     /aws/lambda/scrapeMacroData                               309.2MB  FOREVER
- `01:39:25`     /ecs/openbb-api                                            64.2MB  FOREVER
- `01:39:25`     /aws/lambda/justhodl-daily-report-v3                       50.5MB  FOREVER
- `01:39:25`     /aws/lambda/justhodl-crypto-intel                          44.3MB  FOREVER
- `01:39:25`     /aws/lambda/justhodl-ultimate-orchestrator                 35.2MB  FOREVER
- `01:39:25`     /aws/lambda/cftc-futures-positioning-agent                 29.8MB  FOREVER
- `01:39:25`     /aws/lambda/openbb-system2-api                             25.0MB  FOREVER
- `01:39:25`     /aws/lambda/fedliquidityapi                                22.8MB  FOREVER
- `01:39:25`     /aws/lambda/bond-indices-agent                             22.3MB  FOREVER
- `01:39:25`     /aws/lambda/aiapi-market-analyzer                          17.6MB  FOREVER
- `01:39:25`     /aws/lambda/global-liquidity-agent-v2                      16.6MB  FOREVER
- `01:39:25`     /aws/lambda/xccy-basis-agent                               14.3MB  FOREVER
- `01:39:25`     /aws/lambda/treasury-api                                   13.2MB  FOREVER
- `01:39:25`     /aws/lambda/coinmarketcap-agent                             8.7MB  FOREVER
- `01:39:25`     /aws/lambda/justhodl-data-collector                         8.3MB  FOREVER
- `01:39:25`     /aws/lambda/justhodl-bloomberg-v8                           8.2MB  FOREVER
- `01:39:25`     /aws/lambda/manufacturing-global-agent                      7.5MB  FOREVER
- `01:39:25`     /aws/lambda/news-sentiment-agent                            6.7MB  FOREVER
- `01:39:25`     /aws/lambda/justhodl-options-flow                           6.7MB  FOREVER
- `01:39:25` 
  Log groups with NO retention policy: 20/107
- `01:39:25`   ↑ Setting these to 14d retention is a common safe cost win.
## 4. S3 bucket sizes

- `01:39:26`   justhodl-dashboard-live: 26.41 GB
- `01:39:26`   Estimated cost: $0.61/mo
## 5. DynamoDB tables (active ones only)

- `01:39:28`   Active tables: 7, empty: 18
- `01:39:28`     fed-liquidity-cache                           19389KB  items=    267828  PAY_PER_REQUEST
- `01:39:28`     justhodl-signals                               2873KB  items=      4779  PAY_PER_REQUEST
- `01:39:28`     justhodl-outcomes                              1701KB  items=      4307  PAY_PER_REQUEST
- `01:39:28`     openbb-historical-data                            1KB  items=         1  PAY_PER_REQUEST
- `01:39:28`     ai-assistant-tasks                                1KB  items=         6  PAY_PER_REQUEST
- `01:39:28`     openbb-trading-signals                            0KB  items=         2  PAY_PER_REQUEST
- `01:39:28`     liquidity-metrics-v2                              0KB  items=         1  PAY_PER_REQUEST
- `01:39:28` 
  Empty tables (cleanup candidates):
- `01:39:28`     APIKeys
- `01:39:28`     MacroMetrics
- `01:39:28`     OpenBBUsers
- `01:39:28`     WebSocketConnections
- `01:39:28`     agent-cache-table
- `01:39:28`     aiapi-market-metadata
- `01:39:28`     autonomous-ai-system-data
- `01:39:28`     autonomous-ai-tasks
- `01:39:28`     bls-data-857687956942-bls-minimal
- `01:39:28`     chatgpt-agent-audit-log
- `01:39:28`     chatgpt-agent-state
- `01:39:28`     chatgpt-state
- `01:39:28`     fed-liquidity-cache-v3
- `01:39:28`     justhodl-historical
- `01:39:28`     liquidity-indicators-v3
- `01:39:28`     liquidity-reversals-v3
- `01:39:28`     openbb-bls-data
- `01:39:28`     openbb-bls-data-857687956942
## 6. High-frequency EventBridge rules (cost driver via downstream Lambda invocations)

- `01:39:28`   Top 15 by frequency:
- `01:39:28`     rate(5 minutes)                        288/day  autonomous-ai-schedule
- `01:39:28`     rate(5 minutes)                        288/day  bloomberg-terminal-refresh
- `01:39:28`     rate(5 minutes)                        288/day  justhodl-flow-refresh
- `01:39:28`     rate(5 minutes)                        288/day  justhodl-v9-auto-refresh
- `01:39:28`     rate(15 minutes)                        96/day  aiapi-market-data-collection
- `01:39:28`     rate(15 minutes)                        96/day  justhodl-crypto-intel-schedule
- `01:39:28`     rate(15 minutes)                        96/day  justhodl-dex-scanner-15min
- `01:39:28`     rate(15 minutes)                        96/day  repo-metrics-15min
- `01:39:28`     rate(30 minutes)                        48/day  news-sentiment-update
- `01:39:28`     rate(30 minutes)                        48/day  xccy-basis-30min
- `01:39:28`     rate(1 hour)                            24/day  aiapi-hourly-collection
- `01:39:28`     rate(1 hour)                            24/day  aiapi-hourly-monitor
- `01:39:28`     rate(1 hour)                            24/day  aiapi-hourly-predictions
- `01:39:28`     rate(1 hour)                            24/day  bond-indices-hourly
- `01:39:28`     rate(1 hour)                            24/day  justhodl-hourly-collection
## Recommendations

