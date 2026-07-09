## 0. Wait for deploys

**Status:** success  
**Duration:** 33.9s  
**Finished:** 2026-07-09T22:36:35+00:00  

## Data

| band | barometer | code_ages_min | dedupe_leaks | funding_family_fallback | funding_green | funding_rows | invoke | mechanisms | n_canaries | n_fails | n_votes | n_warns | note | page_everything_tab | page_gauge | plumbing_firing | plumbing_rows | sentinel_informational | sentinel_rows | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | {'justhodl-canary-warroom': 0.2} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | {'ok': True, 'barometer': 36.7, 'master_ew': 23.9, 'n_firing': 106, 'n_divergences': 1} |  |  |  |  |  |  |  |  |  |  |  |  |  |
| WATCH | 36.7 |  |  | 0 | 29 | 31 |  |  | 272 |  | 272 |  | Every watched canary = one equal vote of its 0-100 stress — including every individual funding-plumbing member and every |  |  |  |  | 18 | 52 |  |
|  |  |  | [] |  |  |  |  | ['macro_grid', 'funding', 'leading_markets', 'dollar', 'vol', 'ciss', 'factor_regime', 'cftc', 'global_stress', 'plumbing', 'alerts'] |  |  |  |  |  |  |  | 15 | 22 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 0 |  | 0 |  |  |  |  |  |  |  | PASS |

## Log
## 1. Warroom v4 regeneration

## 3. Live page checks (CDN lag = warn-level)

- `22:36:35` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `22:36:35` PASS -- barometer 36.7 (WATCH) over 272 equal votes; full inventory live
