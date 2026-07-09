## 0. Wait for deploys

**Status:** success  
**Duration:** 33.2s  
**Finished:** 2026-07-09T23:25:31+00:00  

## Data

| band | barometer | by_mechanism | code_ages_min | funding_family_fallback | funding_green | funding_rows | invoke | n_canaries | n_fails | n_mechanisms | n_votes | n_warns | note | page_everything_tab | page_gauge | pc_band5 | pc_score | pm_band5 | pm_score | sentinel_informational | sentinel_rows | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | {'justhodl-canary-warroom': 0.1} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | {'ok': True, 'barometer': 36.6, 'per_mechanism': 40.5, 'master_ew': 23.9, 'n_firing': 108, 'n_divergences': 1} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| WATCH | 36.6 |  |  | 0 | 29 | 31 |  | 292 |  |  | 292 |  | Every watched canary = one equal vote of its 0-100 stress — including every individual funding-plumbing member and every |  |  |  |  |  |  | 18 | 52 |  |
|  |  | {'macro_grid': 39.9, 'funding': 24.3, 'leading_markets': 44.2, 'dollar': 40.7, 'vol': 38.4, 'ciss': 25.8, 'factor_regime': 25.6, 'cftc': 81.0, 'global_stress': 34.3, 'plumbing': 63.9, 'eurodollar': 35.6, 'alerts': 32.8} |  |  |  |  |  |  |  | 12 |  |  |  |  |  | GUARDED | 36.6 | ELEVATED | 40.5 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | False |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 0 |  |  | 1 |  |  |  |  |  |  |  |  |  | PASS |

## Log
## 1. Warroom v4 regeneration

## 3. Live page checks (CDN lag = warn-level)

- `23:25:31` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `23:25:31` PASS -- barometer 36.6 (WATCH) over 292 equal votes; full inventory live
