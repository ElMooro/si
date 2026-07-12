# ops 3138 — subset-coverage evidence matcher + RORO chip

**Status:** success  
**Duration:** 5.4s  
**Finished:** 2026-07-12T03:37:29+00:00  

## Error

```
SystemExit: 0
```

## Data

| n_cards | n_fails | n_warns | open_calls | quotes | tiers | verdict |
|---|---|---|---|---|---|---|
| 7 |  |  |  |  | {"prior": 2, "magdist": 5} |  |
|  |  |  | 3 | True |  |  |
|  | 0 | 0 |  |  |  | PASS |

## Log
## 1. Deploy (helpers inject aws/shared — no manual shim)

- `03:37:24`   zip: 61429 bytes
## 1. Lambda

- `03:37:24`   Lambda exists — updating
- `03:37:27` ✅   ✓ updated justhodl-alpha-compass
## 2. EB rule + permissions

- `03:37:27`   rule already correct: alpha-compass-3h (cron(50 */3 * * ? *))
- `03:37:27` ✅   ✓ target → justhodl-alpha-compass
- `03:37:27` ✅   ✓ added invoke permission
## 3. Smoke test

- `03:37:27`   invoking justhodl-alpha-compass…
- `03:37:29` ✅   ✓ smoke test passed
- `03:37:29`     ok                       True
- `03:37:29`     cards                    7
- `03:37:29`     regime                   Normal
## 2. Fresh output

- `03:37:29` ✅ fresh doc 2026-07-12T03:37:28.593617+00:00
## 3. Gates

- `03:37:29` ✅ regime=Normal sources=5
- `03:37:29` ✅ RORO chip live: MILD_RISK_ON (24.9)
- `03:37:29` · US equity — positioning: via=['earnings_pead'] n=512 h=21d median=1.038 win=0.533 stop/tgt=-4.607/5.74 kelly=1.38%
- `03:37:29` · Crypto: via=['crypto_risk_score'] n=405 h=14d median=0.0 win=0.319 stop/tgt=-0.553/0.5 kelly=0.0%
- `03:37:29` · Broad risk / equity beta: via=['plumbing_stress'] n=389 h=30d median=0.0 win=0.303 stop/tgt=0.0/0.172 kelly=0.28%
- `03:37:29` · US equity — value tilt: via=['sustained_target_equity'] n=160 h=14d median=0.07 win=0.631 stop/tgt=-0.666/2.002 kelly=1.65%
- `03:37:29` · Cross-asset relative value: via=['correlation_break'] n=768 h=21d median=0.069 win=0.531 stop/tgt=-1.757/1.304 kelly=0.0%
