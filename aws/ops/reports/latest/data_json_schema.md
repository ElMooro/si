# data.json schema audit

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-04-22T22:54:00+00:00  

## Data

| fetch | file | keys_read | num_keys | placeholders |
|---|---|---|---|---|
| s3.get_object (boto3) | aws/lambdas/justhodl-ai-chat/source/lambda_function.py | btc, change_pct, composite_score, current, dominance, fear_greed, generated_at, high… | 29 | [REGIME] |
| urllib | aws/lambdas/justhodl-bloomberg-v8/source/lambda_function.py |  | 0 | — |
| s3.get_object (boto3) | aws/lambdas/justhodl-chat-api/source/lambda_function.py | ath, content, fred, gainers, generated, khalid_index, losers, messages… | 13 | — |
| s3.get_object (boto3) | aws/lambdas/justhodl-crypto-intel/source/lambda_function.py | coins, consensus, khalid_index, ki, metrics, price, regime, risk_score… | 12 | — |
| urllib | aws/lambdas/justhodl-edge-engine/source/lambda_function.py | alerts, c, data, day, http, method, net_liquidity_b, observations… | 13 | — |
| unknown | aws/lambdas/justhodl-intelligence/source/lambda_function.py | at_risk, bottomed, buys, downtrend, fred, gainers, icebofa, losers… | 16 | — |
| s3.get_object (boto3) | aws/lambdas/justhodl-investor-agents/source/lambda_function.py | buy, color, content, conviction, cpi, dcf, epsgrowth, fed_rate… | 35 | — |
| s3.get_object (boto3) | aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py | alerts, carry_risk_score, changes, coins, composite_score, correlation, crisis_distance, crypto… | 35 | [DATA] |
| s3.get_object (boto3) | aws/lambdas/justhodl-news-sentiment/source/lambda_function.py |  | 0 | — |
| urllib | aws/lambdas/justhodl-options-flow/source/lambda_function.py |  | 0 | — |
| unknown | aws/lambdas/justhodl-repo-monitor/source/lambda_function.py | headline, phase | 2 | — |
| s3.get_object (boto3) | aws/lambdas/justhodl-signal-logger/source/lambda_function.py | action, bitcoin, buffett_indicator, buys, cape, changes, composite_score, confidence… | 39 | — |
| s3.get_object (boto3) | aws/lambdas/justhodl-stock-screener/source/lambda_function.py |  | 0 | — |
| urllib | aws/lambdas/justhodl-valuations-agent/source/lambda_function.py | http, method, metrics, pct_above_avg | 4 | — |
| urllib | aws/lambdas/nasdaq-datalink-agent/source/lambda_function.py | column_names, data, dataset_data, http, method, nasdaq | 6 | — |
| fetch() | edge.html | _saved_at, alerts, composite_score, correlation, correlations, earnings, earnings_momentum, edge_score… | 19 | — |
| fetch() | flow.html | fund_flows, gamma_exposure, put_call, sentiment, skew, trading_signals, vix_complex | 7 | — |
| fetch() | liquidity.html | date, value | 2 | — |
| fetch() | macroeconomic-platform.html | fund_flows, put_call, sentiment, trading_signals | 4 | — |
| fetch() | pro.html | daily_chg, price, rsi, trend_score | 4 | — |
| fetch() | valuations.html | all_metrics, composite, crypto, generated, gold_metals, metrics, oil_commodities, sp500… | 9 | — |

## Log
- `22:54:00` Repo root: /home/runner/work/si/si
## Live data.json shape (from S3)

- `22:54:00` ✗ Couldn't fetch live data.json: HTTP Error 403: Forbidden
## Scanning consumers

- `22:54:00` Python files to scan: 78
- `22:54:00` HTML/JS files to scan: 36
- `22:54:00` ✅ Found 21 files that read data.json
## Consumer summary

## Key usage vs. live shape

- `22:54:00` Keys consumers look for: 162
- `22:54:00` Keys producer writes:    0
## ⚠ Consumer expects keys that don't exist in live data.json

- `22:54:00`   `_saved_at` — read by: edge.html
- `22:54:00`   `action` — read by: aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   `alerts` — read by: aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py, aws/lambdas/justhodl-edge-engine/source/lambda_function.py, edge.html
- `22:54:00`   `all_metrics` — read by: valuations.html
- `22:54:00`   `at_risk` — read by: aws/lambdas/justhodl-intelligence/source/lambda_function.py
- `22:54:00`   `ath` — read by: aws/lambdas/justhodl-chat-api/source/lambda_function.py
- `22:54:00`   `bitcoin` — read by: aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   `bottomed` — read by: aws/lambdas/justhodl-intelligence/source/lambda_function.py
- `22:54:00`   `btc` — read by: aws/lambdas/justhodl-ai-chat/source/lambda_function.py
- `22:54:00`   `buffett_indicator` — read by: aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   `buy` — read by: aws/lambdas/justhodl-investor-agents/source/lambda_function.py
- `22:54:00`   `buys` — read by: aws/lambdas/justhodl-intelligence/source/lambda_function.py, aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   `c` — read by: aws/lambdas/justhodl-edge-engine/source/lambda_function.py
- `22:54:00`   `cape` — read by: aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   `carry_risk_score` — read by: aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py
- `22:54:00`   `change_pct` — read by: aws/lambdas/justhodl-ai-chat/source/lambda_function.py
- `22:54:00`   `changes` — read by: aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py, aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   `coins` — read by: aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py, aws/lambdas/justhodl-crypto-intel/source/lambda_function.py
- `22:54:00`   `color` — read by: aws/lambdas/justhodl-investor-agents/source/lambda_function.py
- `22:54:00`   `column_names` — read by: aws/lambdas/nasdaq-datalink-agent/source/lambda_function.py
- `22:54:00`   `composite` — read by: valuations.html
- `22:54:00`   `composite_score` — read by: aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py, aws/lambdas/justhodl-ai-chat/source/lambda_function.py, aws/lambdas/justhodl-signal-logger/source/lambda_function.py (+1)
- `22:54:00`   `confidence` — read by: aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   `consensus` — read by: aws/lambdas/justhodl-crypto-intel/source/lambda_function.py
- `22:54:00`   `content` — read by: aws/lambdas/justhodl-chat-api/source/lambda_function.py, aws/lambdas/justhodl-investor-agents/source/lambda_function.py
- `22:54:00`   `contract` — read by: aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   `conviction` — read by: aws/lambdas/justhodl-investor-agents/source/lambda_function.py
- `22:54:00`   `correlation` — read by: aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py, aws/lambdas/justhodl-signal-logger/source/lambda_function.py, edge.html
- `22:54:00`   `correlations` — read by: edge.html
- `22:54:00`   `cpi` — read by: aws/lambdas/justhodl-investor-agents/source/lambda_function.py
- `22:54:00`   `crisis_distance` — read by: aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py
- `22:54:00`   `crypto` — read by: aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py, valuations.html
- `22:54:00`   `current` — read by: aws/lambdas/justhodl-ai-chat/source/lambda_function.py, aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   `daily_chg` — read by: pro.html
- `22:54:00`   `data` — read by: aws/lambdas/justhodl-edge-engine/source/lambda_function.py, aws/lambdas/nasdaq-datalink-agent/source/lambda_function.py
- `22:54:00`   `dataset_data` — read by: aws/lambdas/nasdaq-datalink-agent/source/lambda_function.py
- `22:54:00`   `date` — read by: liquidity.html
- `22:54:00`   `day` — read by: aws/lambdas/justhodl-edge-engine/source/lambda_function.py
- `22:54:00`   `dcf` — read by: aws/lambdas/justhodl-investor-agents/source/lambda_function.py
- `22:54:00`   `direction` — read by: aws/lambdas/justhodl-signal-logger/source/lambda_function.py
- `22:54:00`   …and 120 more
## ℹ Producer writes keys nobody reads

- `22:54:00`   (none — all producer keys are consumed)
## Placeholders found in consumer code

- `22:54:00` These files contain literal placeholder strings that appear in user-facing output when a key is missing:
- `22:54:00`   - `aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py`: [DATA]
- `22:54:00`   - `aws/lambdas/justhodl-ai-chat/source/lambda_function.py`: [REGIME]
- `22:54:00` Full data: aws/ops/reports/latest/data_json_schema.json
- `22:54:00` Done
