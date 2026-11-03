# ops 6716 - groundwork for the fleet-wide trigger cap (read-only)

**Status:** success  
**Duration:** 49.3s  
**Finished:** 2026-11-03T03:39:48+00:00  

## Log
## A. the breaker table

- `03:38:59` justhodl-event-breaker: keys [('pk', 'HASH')] | attrs [('pk', 'S')] | billing PAY_PER_REQUEST | items 0 | bytes 0 | status ACTIVE | TTL ENABLED on expires_at
- `03:38:59` sample keys: []
## B. execution roles and what IAM allows them

- `03:39:10` 914 functions, 14 roles
- `03:39:47` lambda-execution-role                          900 fns | ddb Update allowed      Get allowed      Scan allowed      | invoke governor allowed      | PutEvents allowed      | e.g. ['MLPredictor', 'alphavantage-market-agent', 'alphavantage-technical-analysis']
- `03:39:47` openbb-lambda-role                               2 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents implicitDeny | e.g. ['ecb-data-daily-updater', 'openbb-system2-api']
- `03:39:47` ofr-lambda-role                                  1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents implicitDeny | e.g. ['ofrapi']
- `03:39:47` justhodl-spend-governor-role                     1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents implicitDeny | e.g. ['justhodl-spend-governor']
- `03:39:47` justhodl-factory-grader-role                     1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents explicitDeny | e.g. ['justhodl-factory-grader']
- `03:39:47` justhodl-student-rsi-role                        1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents explicitDeny | e.g. ['justhodl-student-rsi']
- `03:39:47` liquidity-agent-role-v2                          1 fns | ddb Update allowed      Get allowed      Scan allowed      | invoke governor allowed      | PutEvents allowed      | e.g. ['global-liquidity-agent-v2']
- `03:39:47` scrapeMacroData-role-9pweui52                    1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents implicitDeny | e.g. ['scrapeMacroData']
- `03:39:47` daily-liquidity-role                             1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents implicitDeny | e.g. ['daily-liquidity-report']
- `03:39:47` bls-api-role                                     1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents implicitDeny | e.g. ['bls-employment-api-v2']
- `03:39:47` lambda-role                                      1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents implicitDeny | e.g. ['fedliquidityapi']
- `03:39:47` economyapi-lambda-role                           1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents implicitDeny | e.g. ['fmp-stock-picks-agent']
- `03:39:47` MacroFinancialIntelligenceRole                   1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor implicitDeny | PutEvents implicitDeny | e.g. ['macro-financial-intelligence']
- `03:39:47` chatgpt-agent-self-modify                        1 fns | ddb Update implicitDeny Get implicitDeny Scan implicitDeny | invoke governor allowed      | PutEvents implicitDeny | e.g. ['chatgpt-agent-api']
- `03:39:47` roles of the relay / trigger engines: {'justhodl-event-coordinator': 'lambda-execution-role', 'justhodl-spend-governor': 'justhodl-spend-governor-role', 'justhodl-calibrator': 'lambda-execution-role', 'justhodl-13f-positions': 'lambda-execution-role', 'justhodl-a2a-bus': 'lambda-execution-role', 'justhodl-backend-agent': 'lambda-execution-role', 'justhodl-symdir': 'lambda-execution-role', 'justhodl-tv-bars': 'lambda-execution-role', 'justhodl-fundamental-census': 'lambda-execution-role', 'justhodl-miss-calibrator': 'lambda-execution-role', 'justhodl-outcome-checker': 'lambda-execution-role', 'justhodl-sec-13f': 'lambda-execution-role', 'justhodl-worldbank-full': 'lambda-execution-role', 'justhodl-chain-resumer': 'lambda-execution-role', 'justhodl-sdmx-walker': 'lambda-execution-role', 'justhodl-equity-research': 'lambda-execution-role', 'justhodl-13f-clone-alpha': 'lambda-execution-role', 'justhodl-import-sentinel': 'lambda-execution-role', 'justhodl-stock-screener': 'lambda-execution-role'}
## C. the canary

- `03:39:47` justhodl-spend-governor-canary: 128 MB / 5 s, runtime python3.12, role lambda-execution-role, modified 2026-10-16T20:17, reserved None | ops 6522 - no-op canary for the spend governor's live kill-switch selftest (throttle, verify refusal, restore exactly)
## D. master-ranker schedule as live

- `03:39:48` justhodl-master-ranker-weekday-close: cron(40 16 ? * MON-FRI *) America/New_York ENABLED target justhodl-master-ranker
## E. coordinator + governor as deployed

- `03:39:48` justhodl-event-coordinator: modified 2026-10-17T01:53:13, reserved 0, 256 MB / 60 s, env ['TELEGRAM_BOT_TOKEN']
- `03:39:48` justhodl-spend-governor: modified 2026-11-01T11:41:49, reserved 1, 512 MB / 900 s, env ['ACCOUNT_ID', 'ALERT_TOPIC_ARN', 'BUDGET_NAME', 'CE_INTERVAL_H', 'STATE_BUCKET']
- `03:39:48` justhodl-chain-resumer: modified 2026-09-09T13:29:52, reserved 1, 256 MB / 120 s, env ['HELD_PREFIX', 'LEDGER_PREFIX', 'MAX_PER_RUN', 'PARK_PREFIX', 'RESUME_MAX_PER_DAY', 'S3_BUCKET', 'SNS_ARN']
- `03:39:48` ✅ ops 6716 complete (read-only)
