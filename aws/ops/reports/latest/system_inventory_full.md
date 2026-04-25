# Full system inventory — for canonical architecture doc

**Status:** success  
**Duration:** 7.4s  
**Finished:** 2026-04-25T00:33:58+00:00  

## Data

| ddb_tables | eb_rules | lambdas | s3_objects | ssm_params |
|---|---|---|---|---|
| 25 | 98 | 95 | 5000 | 5 |

## Log
## A. Lambda functions

- `00:33:51`   Total: 95
- `00:33:51`   Naming clusters:
- `00:33:51`     justhodl: 37 (e.g. justhodl-data-collector)
- `00:33:51`     openbb: 3 (e.g. openbb-websocket-broadcast)
- `00:33:51`     macro: 3 (e.g. macro-report-api)
- `00:33:51`     ecb: 3 (e.g. ecb-data-daily-updater)
- `00:33:51`     bls: 2 (e.g. bls-labor-agent)
- `00:33:51`     fedliquidityapi: 2 (e.g. fedliquidityapi-test)
- `00:33:51`     fmp: 2 (e.g. fmp-fundamentals-agent)
- `00:33:51`     treasury: 2 (e.g. treasury-auto-updater)
- `00:33:51`     global: 2 (e.g. global-liquidity-agent-v2)
- `00:33:51`     alphavantage: 2 (e.g. alphavantage-market-agent)
- `00:33:51`     nyfed: 2 (e.g. nyfed-primary-dealer-fetcher)
- `00:33:51`     ofrapi: 1 (e.g. ofrapi)
- `00:33:51`     volatility: 1 (e.g. volatility-monitor-agent)
- `00:33:51`     bond: 1 (e.g. bond-indices-agent)
- `00:33:51`     google: 1 (e.g. google-trends-agent)
- `00:33:51`     OpenBBS3DataProxy: 1 (e.g. OpenBBS3DataProxy)
- `00:33:51`     eia: 1 (e.g. eia-energy-agent)
- `00:33:51`     FinancialIntelligence: 1 (e.g. FinancialIntelligence-Backend)
- `00:33:51`     createEnhancedIndex: 1 (e.g. createEnhancedIndex)
- `00:33:51`     census: 1 (e.g. census-economic-agent)
- `00:33:51`     dollar: 1 (e.g. dollar-strength-agent)
- `00:33:51`     economyapi: 1 (e.g. economyapi)
- `00:33:51`     aiapi: 1 (e.g. aiapi-market-analyzer)
- `00:33:51`     autonomous: 1 (e.g. autonomous-ai-processor)
- `00:33:51`     permanent: 1 (e.g. permanent-market-intelligence)
- `00:33:51`     ultimate: 1 (e.g. ultimate-multi-agent)
- `00:33:51`     scrapeMacroData: 1 (e.g. scrapeMacroData)
- `00:33:51`     daily: 1 (e.g. daily-liquidity-report)
- `00:33:51`     fred: 1 (e.g. fred-ice-bofa-api)
- `00:33:51`     MLPredictor: 1 (e.g. MLPredictor)
- `00:33:51`     cftc: 1 (e.g. cftc-futures-positioning-agent)
- `00:33:51`     xccy: 1 (e.g. xccy-basis-agent)
- `00:33:51`     coinmarketcap: 1 (e.g. coinmarketcap-agent)
- `00:33:51`     multi: 1 (e.g. multi-agent-orchestrator)
- `00:33:51`     benzinga: 1 (e.g. benzinga-news-agent)
- `00:33:51`     securities: 1 (e.g. securities-banking-agent)
- `00:33:51`     testEnhancedScraper: 1 (e.g. testEnhancedScraper)
- `00:33:51`     createUniversalIndex: 1 (e.g. createUniversalIndex)
- `00:33:51`     enhanced: 1 (e.g. enhanced-repo-agent)
- `00:33:51`     manufacturing: 1 (e.g. manufacturing-global-agent)
- `00:33:51`     bea: 1 (e.g. bea-economic-agent)
- `00:33:51`     news: 1 (e.g. news-sentiment-agent)
- `00:33:51`     nyfedapi: 1 (e.g. nyfedapi-isolated)
- `00:33:51`     nasdaq: 1 (e.g. nasdaq-datalink-agent)
- `00:33:51`     chatgpt: 1 (e.g. chatgpt-agent-api)
- `00:33:51`     universal: 1 (e.g. universal-agent-gateway)
## B. S3 keys (justhodl-dashboard-live)

- `00:33:52`   Total objects (capped): 5000
- `00:33:52`   Top-level directories:
- `00:33:52`     data/ — 3319 files, 5538081KB, newest 1125.0h old
- `00:33:52`     archive/ — 1665 files, 29363KB, newest 0.3h old
- `00:33:52`     calibration/ — 7 files, 24KB, newest 135.6h old
- `00:33:52`     bot/ — 1 files, 28KB, newest 1145.4h old
- `00:33:52`   Root-level files: 8
- `00:33:52`     crypto-intel.json                                  55889 bytes  (   0.2h)
- `00:33:52`     crypto.html                                        43374 bytes  (1146.9h)
- `00:33:52`     benzinga.html                                       5628 bytes  (1336.9h)
- `00:33:52`     crypto-data.json                                   40110 bytes  (1341.2h)
- `00:33:52`     ath.html                                           15998 bytes  (1385.6h)
- `00:33:52`     charts.html                                       245035 bytes  (1435.1h)
- `00:33:52`     data-peek.json                                     60635 bytes  (1456.4h)
- `00:33:52`     data.json                                          60635 bytes  (1571.5h)
## C. DynamoDB tables

- `00:33:53`   fed-liquidity-cache                      items=    267828 size=     19389KB billing=PAY_PER_REQUEST
- `00:33:53`   justhodl-signals                         items=      4579 size=      2211KB billing=PAY_PER_REQUEST
- `00:33:53`   justhodl-outcomes                        items=       738 size=       297KB billing=PAY_PER_REQUEST
- `00:33:53`   openbb-historical-data                   items=         1 size=         1KB billing=PAY_PER_REQUEST
- `00:33:53`   ai-assistant-tasks                       items=         6 size=         1KB billing=PAY_PER_REQUEST
- `00:33:53`   openbb-trading-signals                   items=         2 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   liquidity-metrics-v2                     items=         1 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   APIKeys                                  items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   MacroMetrics                             items=         0 size=         0KB billing=PROVISIONED
- `00:33:53`   OpenBBUsers                              items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   WebSocketConnections                     items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   agent-cache-table                        items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   aiapi-market-metadata                    items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   autonomous-ai-system-data                items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   autonomous-ai-tasks                      items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   bls-data-857687956942-bls-minimal        items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   chatgpt-agent-audit-log                  items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   chatgpt-agent-state                      items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   chatgpt-state                            items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   fed-liquidity-cache-v3                   items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   justhodl-historical                      items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   liquidity-indicators-v3                  items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   liquidity-reversals-v3                   items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   openbb-bls-data                          items=         0 size=         0KB billing=PAY_PER_REQUEST
- `00:33:53`   openbb-bls-data-857687956942             items=         0 size=         0KB billing=PAY_PER_REQUEST
## D. SSM parameters under /justhodl/

- `00:33:53`   Total: 5
- `00:33:53`   /justhodl/ai-chat/auth-token                                 type=SecureString age= 57.1h
- `00:33:53`   /justhodl/calibration/accuracy                               type=String     age=135.6h
- `00:33:53`   /justhodl/calibration/report                                 type=String     age=135.6h
- `00:33:53`   /justhodl/calibration/weights                                type=String     age=135.6h
- `00:33:53`   /justhodl/telegram/chat_id                                   type=String     age=1056.2h
## E. EventBridge rules

- `00:33:57`   Total: 98
- `00:33:57`   Enabled: 90, disabled: 8
- `00:33:57` 
  Schedule summary (top 30 by count):
- `00:33:57`     aiapi-market-analyzer                              4 rule(s): ['cron(0 7 * * ? *)', 'rate(1 hour)']
- `00:33:57`     fmp-stock-picks-agent                              3 rule(s): ['cron(0 14,16,18,20 ? * MON-FRI *)', 'cron(0 12 ? * MON-FRI *)']
- `00:33:57`     justhodl-outcome-checker                           3 rule(s): ['cron(30 22 ? * MON-FRI *)', 'cron(0 8 1 * ? *)']
- `00:33:57`     justhodl-daily-report-v3                           3 rule(s): ['rate(5 minutes)', 'cron(0 23 ? * MON-FRI *)']
- `00:33:57`     global-liquidity-agent-v2                          2 rule(s): ['cron(0 12 * * ? *)', 'cron(0 13 * * ? *)']
- `00:33:57`     aiapi-monitor                                      2 rule(s): ['cron(0 14 * * ? *)', 'rate(1 hour)']
- `00:33:57`     bls-employment-api-v2                              2 rule(s): ['cron(0 22 ? * FRI *)', 'cron(0 22 ? * TUE *)']
- `00:33:57`     fredapi                                            2 rule(s): ['cron(0 13 ? * MON,WED,FRI *)', 'cron(0 14 ? * MON *)']
- `00:33:57`     fedapi                                             2 rule(s): ['cron(0 14 ? * MON *)', 'cron(0 14 ? * THU *)']
- `00:33:57`     justhodl-intelligence                              2 rule(s): ['cron(10 12 * * ? *)', 'cron(5 12-23 ? * MON-FRI *)']
- `00:33:57`     justhodl-repo-monitor                              2 rule(s): ['cron(0/30 13-23 ? * MON-FRI *)', 'cron(0 12 * * ? *)']
- `00:33:57`     ofrapi                                             2 rule(s): ['cron(0 14 * * ? *)', 'cron(0 13 ? * MON *)']
- `00:33:57`     treasury-auto-updater                              2 rule(s): ['cron(0 10 ? * MON *)', 'cron(0 10 ? * THU *)']
- `00:33:57`     justhodl-email-reports-v2                          1 rule(s): ['cron(0 12 * * ? *)']
- `00:33:57`     justhodl-daily-macro-report                        1 rule(s): ['cron(0 12 * * ? *)']
- `00:33:57`     scrapeMacroData                                    1 rule(s): ['cron(0 12 * * ? *)']
- `00:33:57`     MLPredictor                                        1 rule(s): ['cron(15 12 * * ? *)']
- `00:33:57`     alphavantage-market-agent                          1 rule(s): ['cron(*/15 13-21 ? * MON-FRI *)']
- `00:33:57`     autonomous-ai-processor                            1 rule(s): ['rate(5 minutes)']
- `00:33:57`     justhodl-bloomberg-v8                              1 rule(s): ['rate(5 minutes)']
- `00:33:57`     bls-labor-agent                                    1 rule(s): ['cron(30 13 * * ? *)']
- `00:33:57`     bond-indices-agent                                 1 rule(s): ['rate(1 hour)']
- `00:33:57`     cftc-futures-positioning-agent                     1 rule(s): ['cron(0 18 ? * FRI *)']
- `00:33:57`     report-email-agent                                 1 rule(s): ['cron(0 13 * * ? *)']
- `00:33:57`     daily-liquidity-report                             1 rule(s): ['cron(45 12 * * ? *)']
- `00:33:57`     market-report-generator                            1 rule(s): ['cron(0 13 * * ? *)']
- `00:33:57`     ecb-data-daily-updater                             1 rule(s): ['cron(0 6 * * ? *)']
- `00:33:57`     ecb-auto-updater                                   1 rule(s): ['cron(0 6 ? * MON *)']
- `00:33:57`     fed-liquidity-indicators                           1 rule(s): ['cron(0 14 ? * MON *)']
- `00:33:57`     fedliquidity                                       1 rule(s): ['cron(0 14 ? * MON *)']
- `00:33:58` ✅   Saved structured inventory to s3://justhodl-dashboard-live/_audit/inventory_2026-04-25.json
- `00:33:58` ✅   Saved to repo: aws/ops/audit/inventory_2026-04-25.json
- `00:33:58` Done
