# ops 3166 — Thesis Engine

**Status:** success  
**Duration:** 26.1s  
**Finished:** 2026-07-12T21:30:05+00:00  

## Error

```
SystemExit: 0
```

## Data

| elapsed_s | fdr_survivors | n_fails | n_fdr_survivors_doc | n_theses | n_warns | signals_logged | spy_base_21d | status | theses_firing_now | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 19.6 |  |  |  | 56 |  | 0 | 1.36 | LIVE |  |  |
|  | 0 |  | 0 |  |  |  |  |  | 24 |  |
|  |  | 0 |  |  | 1 |  |  |  |  | PASS |

## Log
## 1. Deploy

- `21:29:40` env keys: ['FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET']
- `21:29:40`   zip: 59391 bytes
## 1. Lambda

- `21:29:40`   Lambda exists — updating
- `21:29:43` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `21:29:44`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `21:29:44` ✅   ✓ target → justhodl-thesis-engine
- `21:29:44` ✅   ✓ added invoke permission
## 2. First run (cold: full history backfill)

- `21:29:44` async invoke fired — backfilling ~550 FRED + ~1,700 Polygon series, then event-studying every thesis
## 3. Results

- `21:30:05` ── ranked by OVERLAP-CORRECTED t (FDR survivors first):
- `21:30:05`   Business Cycle                         act  25.0% | excess   2.36% hit-edge  22.1pp t=  1.87 n_eff=  6.7
- `21:30:05`   Consumers                              act  18.2% | excess   1.48% hit-edge  21.2pp t=  1.35 n_eff=  6.0
- `21:30:05`   Employment                             act  66.7% | excess   2.58% hit-edge  12.3pp t=  1.32 n_eff=  4.6
- `21:30:05`   Economy                                act  44.8% | excess   2.62% hit-edge  26.8pp t=  1.65 n_eff=  4.6
- `21:30:05`   Country ETFs                           act  45.5% | excess   0.33% hit-edge  11.7pp t=  0.16 n_eff=  4.7
- `21:30:05`   Credit Spreads                         act  30.0% | excess    1.9% hit-edge  18.5pp t=   1.4 n_eff=  6.7 ★FIRING
- `21:30:05`   Global Credit                          act  70.0% | excess   1.19% hit-edge  19.1pp t=  0.84 n_eff=  4.4 ★FIRING
- `21:30:05`   Bonds - Corp: AGG IS THE BEST TOOL TO  act  69.4% | excess   1.44% hit-edge  19.3pp t=  0.92 n_eff=  4.5 ★FIRING
- `21:30:05`   Financial Conditions                   act  36.4% | excess   2.04% hit-edge  20.1pp t=  1.34 n_eff=  5.4 ★FIRING
- `21:30:05`   Corp Yields                            act  50.0% | excess   1.28% hit-edge  17.2pp t=  1.02 n_eff=  6.4 ★FIRING
- `21:30:05`   Global Bonds                           act  58.3% | excess   1.68% hit-edge  19.6pp t=  1.03 n_eff=  4.6 ★FIRING
- `21:30:05`   Credit Risk                            act  20.0% | excess   1.91% hit-edge  17.1pp t=   1.2 n_eff=  5.2
- `21:30:05`   DXY predict Future Moves : Currencies  act  55.2% | excess   0.96% hit-edge  25.4pp t=  1.13 n_eff=  5.3 ★FIRING
- `21:30:05`   Credit market                          act  48.3% | excess   1.81% hit-edge  19.2pp t=  1.08 n_eff=  4.5 ★FIRING
- `21:30:05` ── SPY base rates (the bar every thesis must clear): 5d 0.33% · 21d 1.36% · 63d 4.29%
- `21:30:05` ✅ 56 theses scored · 0 survive BH-FDR q=0.10 with overlap-corrected t · 24 firing
## 4. Page

- `21:30:05` ✅ theses.html live on CDN
- `21:30:05` ⚠ ZERO theses survive FDR — the honest answer: on ~2y of history none of the 56 shows a 21d lead over SPY that beats multiple testing. The naive t=8.59 from the first run was overlap inflation.
