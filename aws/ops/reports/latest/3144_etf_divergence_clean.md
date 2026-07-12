# ops 3144 — Flow-Price Divergence layer

**Status:** success  
**Duration:** 8.6s  
**Finished:** 2026-07-12T04:18:07+00:00  

## Error

```
SystemExit: 0
```

## Data

| capitulation | clean_candidates | distribution | etfs_ok | n_fails | n_scored | n_warns | quadrant_coverage_pct | signals_logged | signals_logged_v2 | stealth | trend_confirmed | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 6 |  | 1 | 300 |  | 286 |  | 100.0 | 0 |  | 0 | 5 |  |
|  | 0 |  |  |  |  |  |  |  | 0 |  |  |  |
|  |  |  |  | 0 |  | 0 |  |  |  |  |  | PASS |

## Log
## 1. Deploy (env preserved from live function)

- `04:17:58` preserving env keys: ['POLYGON_KEY']
- `04:17:58`   zip: 67836 bytes
## 1. Lambda

- `04:17:59`   Lambda exists — updating
- `04:18:02` ✅   ✓ updated justhodl-etf-fund-flows
## 2. EB rule + permissions

- `04:18:03`   rule already correct: justhodl-etf-fund-flows-daily (cron(0 22 * * ? *))
- `04:18:03` ✅   ✓ target → justhodl-etf-fund-flows
- `04:18:03` ✅   ✓ added invoke permission
## 3. Smoke test

- `04:18:03`   invoking justhodl-etf-fund-flows…
- `04:18:06` ✅   ✓ smoke test passed
- `04:18:06`     ok                       True
- `04:18:06`     elapsed_s                1.8
- `04:18:06`     n_etfs_ok                300
- `04:18:06`     regime                   TRANSITION
## 2. Fresh composite with divergence board

## 3. Gates

- `04:18:07` ✅ quadrant coverage 100.0% of 300 ETFs
- `04:18:07`   distrib: WCLD (cloud) z=-1.32 ret21d=7.28% aum21d=-13.35% score=0.95
- `04:18:07` ✅ leveraged extremes split out: 8 (GDXU, AMUU, BOIL, KOLD)
- `04:18:07` ✅ legacy metric fields intact (industry-rotation join safe)
