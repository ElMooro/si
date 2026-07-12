# ops 3136 — Alpha Compass polish (regime label + quotes)

**Status:** success  
**Duration:** 8.6s  
**Finished:** 2026-07-12T03:29:51+00:00  

## Error

```
SystemExit: 0
```

## Data

| n_fails | n_warns | open_calls | quotes_available | verdict |
|---|---|---|---|---|
|  |  | 3 | True |  |
| 0 | 0 |  |  | PASS |

## Log
## 1. Deploy (shim-injected)

- `03:29:42` injected aws/shared/_sentry_lite.py
- `03:29:42`   zip: 11546 bytes
## 1. Lambda

- `03:29:43`   Lambda exists — updating
- `03:29:49` ✅   ✓ updated justhodl-alpha-compass
## 2. EB rule + permissions

- `03:29:49`   rule already correct: alpha-compass-3h (cron(50 */3 * * ? *))
- `03:29:49` ✅   ✓ target → justhodl-alpha-compass
- `03:29:49` ✅   ✓ added invoke permission
## 3. Smoke test

- `03:29:49`   invoking justhodl-alpha-compass…
- `03:29:51` ✅   ✓ smoke test passed
- `03:29:51`     ok                       True
- `03:29:51`     cards                    7
- `03:29:51`     regime                   Normal
## 2. Fresh output

- `03:29:51` ✅ fresh doc 2026-07-12T03:29:50.517561+00:00
## 3. Strict gates

- `03:29:51` ✅ regime label clean: 'Normal' (score=54.7, sources=4)
- `03:29:51`   · Regime Composite: NORMAL (54.7)
- `03:29:51`   · Factor Regime: 1 style thrusts live 
- `03:29:51`   · Risk-Asset Transmission: LEAN DUMP (-16.0)
- `03:29:51`   · Engine Book: NEUTRAL / MIXED (-0.14)
- `03:29:51` ✅ Lambda-side FMP quotes live
- `03:29:51` ✅ track-record history seeded — 3 entries (3 open)
- `03:29:51`   #1 US macro / housing cycle conv=35 tier=prior kelly=0.7% primary=ITB
- `03:29:51`   #2 US equity — positioning conv=35 tier=scorecard kelly=0.39% primary=IWM
- `03:29:51`   #3 Crypto conv=20 tier=scorecard kelly=0.23% primary=IBIT
