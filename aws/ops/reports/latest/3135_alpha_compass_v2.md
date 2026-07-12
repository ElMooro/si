# ops 3135 — Alpha Compass v2.0 (desk sheet)

**Status:** success  
**Duration:** 7.7s  
**Finished:** 2026-07-12T03:25:30+00:00  

## Error

```
SystemExit: 0
```

## Data

| graded_this_run | n_fails | n_warns | tiers | top_calls | track_open | track_quotes | verdict | watchlist |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 3 |  |  |  | 4 |
|  |  |  | {"prior": 2, "scorecard": 5} |  |  |  |  |  |
| 0 |  |  |  |  | 0 | False |  |  |
|  | 0 | 1 |  |  |  |  | PASS |  |

## Log
## 1. FMP key sourcing + deploy

- `03:25:22` ✅ FMP key live (SPY=754.95)
- `03:25:22` injected aws/shared/_sentry_lite.py into zip source
- `03:25:22`   zip: 11318 bytes
## 1. Lambda

- `03:25:22`   Lambda exists — updating
- `03:25:27` ✅   ✓ updated justhodl-alpha-compass
## 2. EB rule + permissions

- `03:25:28`   rule already correct: alpha-compass-3h (cron(50 */3 * * ? *))
- `03:25:28` ✅   ✓ target → justhodl-alpha-compass
- `03:25:28` ✅   ✓ added invoke permission
## 3. Smoke test

- `03:25:28`   invoking justhodl-alpha-compass…
- `03:25:29` ✅   ✓ smoke test passed
- `03:25:29`     ok                       True
- `03:25:29`     cards                    7
- `03:25:29`     regime                   {'Beta Tilt': 'Neutral To Long', 'Size Mult': 1.018, 'Hedge': 'Normal', 'Crossborder Warning': 'Em Capital Outflow'}
## 2. Poll S3 for fresh v2 output

- `03:25:29` ✅ fresh v2 doc generated_at=2026-07-12T03:25:28.913404+00:00 elapsed_s=0.71
## 3. Quality gates

- `03:25:29` ✅ regime={'Beta Tilt': 'Neutral To Long', 'Size Mult': 1.018, 'Hedge': 'Normal', 'Crossborder Warning': 'Em Capital Outflow'} sources=5 risk_mult=1.0
- `03:25:29`   #1 US macro / housing cycle conv=35 tier=prior kelly=0.7% primary=ITB
- `03:25:29`   #2 US equity — positioning conv=35 tier=scorecard kelly=0.39% primary=IWM
- `03:25:29`   #3 Crypto conv=20 tier=scorecard kelly=0.23% primary=IBIT
- `03:25:29` ✅ history object live — 0 entries
## 4. Page cutover (CDN, warn-only)

- `03:25:30` ✅ justhodl.ai/alpha-compass.html serves the v2 page
- `03:25:30` ⚠ quotes unavailable at runtime despite key — check FMP from Lambda network
