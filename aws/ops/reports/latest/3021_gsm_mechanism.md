## 0. Wait for deploys

**Status:** success  
**Duration:** 14.0s  
**Finished:** 2026-07-09T22:08:00+00:00  

## Data

| band | barometer | code_ages_min | funding_family_fallback | funding_green | funding_rows | gsm_markets | gsm_rows | gsm_tiers | invoke | mechanisms | n_canaries | n_fails | n_votes | n_warns | note | page_everything_tab | page_gauge | sentinel_informational | sentinel_rows | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | {'justhodl-canary-warroom': 0.1} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | {'ok': True, 'barometer': 34.3, 'master_ew': 23.9, 'n_firing': 91, 'n_divergences': 1} |  |  |  |  |  |  |  |  |  |  |  |
| WATCH | 34.3 |  | 0 | 29 | 31 |  |  |  |  |  | 250 |  | 250 |  | Every watched canary = one equal vote of its 0-100 stress — including every individual funding-plumbing member and every |  |  | 18 | 52 |  |
|  |  |  |  |  |  | 13 | 20 | 7 |  | ['macro_grid', 'funding', 'leading_markets', 'dollar', 'vol', 'ciss', 'factor_regime', 'cftc', 'global_stress', 'alerts'] |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 0 |  | 0 |  |  |  |  |  | PASS |

## Log
## 1. Warroom v4 regeneration

## 3. Live page checks (CDN lag = warn-level)

- `22:08:00` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `22:08:00` PASS -- barometer 34.3 (WATCH) over 250 equal votes; full inventory live
