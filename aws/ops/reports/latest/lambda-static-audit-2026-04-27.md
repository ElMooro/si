# Lambda static audit — 2026-04-27

Static-only audit of all 110 Lambda sources in `aws/lambdas/`.

## Summary

| Class | Count |
|-------|-------|
| P0 | 17 |
| P1 | 13 |
| P2 | 2 |
| OK | 78 |


## P0 (17)

### `global-liquidity-agent-v2` [python3.11]
- **P0** — source file for handler 'khalid_no_email.lambda_handler' not found

### `justhodl-asymmetric-scorer` [?]
- **P0** — source file for handler '?' not found

### `justhodl-auction-crisis-detector` [?]
- **P0** — source file for handler '?' not found

### `justhodl-bond-regime-detector` [?]
- **P0** — source file for handler '?' not found

### `justhodl-correlation-breaks` [?]
- **P0** — source file for handler '?' not found

### `justhodl-cot-extremes-scanner` [?]
- **P0** — source file for handler '?' not found

### `justhodl-divergence-scanner` [?]
- **P0** — source file for handler '?' not found

### `justhodl-health-monitor` [?]
- **P0** — source file for handler '?' not found

### `justhodl-pnl-tracker` [?]
- **P0** — source file for handler '?' not found

### `justhodl-prompt-iterator` [?]
- **P0** — source file for handler '?' not found

### `justhodl-reports-builder` [?]
- **P0** — source file for handler '?' not found

### `justhodl-risk-sizer` [?]
- **P0** — source file for handler '?' not found

### `justhodl-watchlist-debate` [?]
- **P0** — source file for handler '?' not found

### `multi-agent-orchestrator` [python3.11]
- **P0** — source file for handler 'lambda_function.lambda_handler' not found

### `news-sentiment-agent` [python3.9]
- **P0** — source file for handler 'lambda_news_agent.lambda_handler' not found

### `treasury-auto-updater` [python3.9]
- **P0** — source file for handler 'updater.lambda_handler' not found

### `ultimate-multi-agent` [python3.11]
- **P0** — source file for handler 'lambda_function.lambda_handler' not found


## P1 (13)

### `FinancialIntelligence-Backend` [python3.11]
- **P1** — hardcoded API key: API_KEY = "2f057499936072679d8...

### `bea-economic-agent` [python3.9]
- **P1** — hardcoded API key: api_key = "997E5691-4F0E-4774-...

### `bls-labor-agent` [python3.9]
- **P1** — hardcoded API key: api_key = "a759447531f04f1f861...

### `census-economic-agent` [python3.9]
- **P1** — hardcoded API key: api_key = "8423ffa543d0e95cdba...

### `coinmarketcap-agent` [python3.9]
- **P1** — hardcoded API key: api_key = '17ba8e87-53f0-46f4-...

### `economyapi` [python3.9]
- **P1** — hardcoded API key: API_KEY = "2f057499936072679d8...

### `fedliquidityapi` [python3.9]
- **P1** — hardcoded API key: API_KEY = '2f057499936072679d8...

### `fedliquidityapi-test` [python3.12]
- **P1** — hardcoded API key: API_KEY = "2f057499936072679d8...

### `justhodl-crypto-intel` [python3.12]
- **P1** — references blocked endpoint api.binance.com: Binance Spot — HTTP 451 from AWS us-east-1 (verified 2026-04)
- **P1** — references blocked endpoint fapi.binance.com: Binance Futures — HTTP 451 from AWS us-east-1
- **P1** — references blocked endpoint api1.binance.com: Binance mirror — HTTP 451
- **P1** — references blocked endpoint api2.binance.com: Binance mirror — HTTP 451
- **P1** — references blocked endpoint api3.binance.com: Binance mirror — HTTP 451
- **P1** — references blocked endpoint data-api.binance.vision: Binance vision mirror — HTTP 451

### `justhodl-daily-report-v3` [python3.12]
- **P1** — hardcoded API key: API_KEY = '17d36cdd13c44e13985...
- **P2** — large source (97 KB) — may exceed Lambda inline size limit; consider splitting

### `justhodl-news-sentiment` [python3.11]
- **P1** — hardcoded API key: API_KEY   = "17d36cdd13c44e139...

### `macro-report-api` [python3.9]
- **P1** — hardcoded API key: API_KEY = '8e42b7b0d4754c0e5e8...

### `openbb-system2-api` [python3.9]
- **P1** — hardcoded API key: api_key = "2f057499936072679d8...


## P2 (2)

### `justhodl-financial-secretary` [python3.12]
- **P2** — silent except-pass blocks: 5 (errors swallowed)

### `justhodl-intelligence` [python3.12]
- **P2** — silent except-pass blocks: 4 (errors swallowed)

