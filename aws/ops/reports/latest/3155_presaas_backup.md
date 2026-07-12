# ops 3155 — pre-SaaS backup

**Status:** success  
**Duration:** 11.5s  
**Finished:** 2026-07-12T18:09:23+00:00  

## Error

```
SystemExit: 0
```

## Data

| ddb_backups | ddb_tables | export_bytes | export_key | lambdas_exported | n_fails | n_warns | ssm_params | verdict |
|---|---|---|---|---|---|---|---|---|
| 14 | 14 |  |  |  |  |  |  |  |
|  |  | 17781 | backups/2026-07-12/lambda-fleet-config.json.gz | 647 |  |  |  |  |
|  |  |  |  |  |  |  | 98 |  |
|  |  |  |  |  | 0 | 0 |  | PASS |

## Log
## 1. Private backup bucket

- `18:09:12` ✅ bucket created
- `18:09:12` ✅ public access blocked + SSE-AES256 enforced
## 2. DynamoDB on-demand backups

- `18:09:15` tables: justhodl-alert-actions, justhodl-api-keys, justhodl-api-rate, justhodl-backtest, justhodl-feedback, justhodl-history, justhodl-llm-cost, justhodl-outcomes, justhodl-portfolio, justhodl-push-subscriptions, justhodl-signal-registry, justhodl-signals, justhodl-subscribers, justhodl-trades
- `18:09:15` ✅ all 14 tables backed up
## 3. Lambda fleet export (config + env)

- `18:09:22` ✅ 647 function configs + env vars secured
## 4. Versioning on live bucket

- `18:09:22` ✅ versioning already Enabled
## 5. SSM /justhodl inventory (names only)

- `18:09:23` all: /justhodl/13f-price-divergence/state, /justhodl/52wk-quality-breakout/state, /justhodl/ai-chat/auth-token, /justhodl/alerts/webhook_urls, /justhodl/anthropic/api-key, /justhodl/anthropic/api_key, /justhodl/api-admin/token, /justhodl/bea-api-key, /justhodl/bls-api-key, /justhodl/brain/uid, /justhodl/breadth-divergence/state, /justhodl/breadth-thrust/state, /justhodl/buyback-yield-ranking/state, /justhodl/calibration-fleet/weights, /justhodl/calibration/accuracy, /justhodl/calibration/alpha, /justhodl/calibration/miss_recurring_tickers, /justhodl/calibration/miss_threshold_proposals, /justhodl/calibration/report, /justhodl/calibration/scorecard, /justhodl/calibration/strategy-weights, /justhodl/calibration/weights, /justhodl/calibration/weights/day_1, /justhodl/calibration/weights/day_10, /justhodl/calibration/weights/day_14, /justhodl/calibration/weights/day_21, /justhodl/calibration/weig
- `18:09:23` ⚠ no supabase/stripe params under /justhodl — service keys live elsewhere (checkout lambda env? — fleet export above now holds every env var)
