# ops 3143 — Flow-Price Divergence layer

**Status:** success  
**Duration:** 10.4s  
**Finished:** 2026-07-12T04:15:45+00:00  

## Error

```
SystemExit: 0
```

## Data

| capitulation | distribution | etfs_ok | n_fails | n_scored | n_warns | quadrant_coverage_pct | signals_logged | stealth | trend_confirmed | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 6 | 5 | 300 |  | 286 |  | 100.0 | 0 | 4 | 5 |  |
|  |  |  | 0 |  | 0 |  |  |  |  | PASS |

## Log
## 1. Deploy (env preserved from live function)

- `04:15:35` preserving env keys: ['POLYGON_KEY']
- `04:15:35`   zip: 67575 bytes
## 1. Lambda

- `04:15:35`   Lambda exists — updating
- `04:15:41` ✅   ✓ updated justhodl-etf-fund-flows
## 2. EB rule + permissions

- `04:15:42`   rule already correct: justhodl-etf-fund-flows-daily (cron(0 22 * * ? *))
- `04:15:42` ✅   ✓ target → justhodl-etf-fund-flows
- `04:15:42` ✅   ✓ added invoke permission
## 3. Smoke test

- `04:15:42`   invoking justhodl-etf-fund-flows…
- `04:15:45` ✅   ✓ smoke test passed
- `04:15:45`     ok                       True
- `04:15:45`     elapsed_s                1.9
- `04:15:45`     n_etfs_ok                300
- `04:15:45`     regime                   TRANSITION
## 2. Fresh composite with divergence board

## 3. Gates

- `04:15:45` ✅ quadrant coverage 100.0% of 300 ETFs
- `04:15:45`   stealth: GDXU (2x_goldminers_bull) z=7.45 ret21d=-17.88% aum21d=4.56% score=7.28
- `04:15:45`   stealth: BOIL (2x_natgas_bull) z=2.58 ret21d=-21.53% aum21d=21.21% score=2.56
- `04:15:45`   stealth: UCO (2x_crude_bull) z=1.61 ret21d=-22.85% aum21d=9.61% score=1.6
- `04:15:45`   stealth: TMF (3x_treasury) z=1.47 ret21d=-3.17% aum21d=-8.25% score=0.55
- `04:15:45`   distrib: GGLS (1x_googl_bear) z=-1.31 ret21d=2.25% aum21d=11.58% score=0.36
- `04:15:45`   distrib: WCLD (cloud) z=-1.32 ret21d=7.28% aum21d=-13.35% score=0.95
- `04:15:45`   distrib: MSTZ (1x_mstr_bear) z=-1.62 ret21d=44.7% aum21d=-100.6% score=1.62
- `04:15:45`   distrib: KOLD (2x_natgas_bear) z=-1.7 ret21d=22.24% aum21d=-35.26% score=1.69
- `04:15:45`   distrib: AMUU (2x_amzn_bull) z=-2.73 ret21d=33.74% aum21d=-9.13% score=2.73
- `04:15:45` ✅ legacy metric fields intact (industry-rotation join safe)
