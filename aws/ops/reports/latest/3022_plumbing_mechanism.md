## 0. Wait for deploys

**Status:** failure  
**Duration:** 33.4s  
**Finished:** 2026-07-09T22:24:49+00:00  

## Error

```
SystemExit: 1
```

## Data

| band | barometer | code_ages_min | dedupe_leaks | funding_family_fallback | funding_green | funding_rows | invoke | mechanisms | n_canaries | n_fails | n_votes | n_warns | note | page_everything_tab | page_gauge | plumbing_firing | plumbing_rows | sentinel_informational | sentinel_rows | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | {'justhodl-canary-warroom': 0.1} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | {'ok': True, 'barometer': 34.3, 'master_ew': 23.9, 'n_firing': 91, 'n_divergences': 1} |  |  |  |  |  |  |  |  |  |  |  |  |  |
| WATCH | 34.3 |  |  | 0 | 29 | 31 |  |  | 250 |  | 250 |  | Every watched canary = one equal vote of its 0-100 stress — including every individual funding-plumbing member and every |  |  |  |  | 18 | 52 |  |
|  |  |  | [] |  |  |  |  | ['macro_grid', 'funding', 'leading_markets', 'dollar', 'vol', 'ciss', 'factor_regime', 'cftc', 'global_stress', 'plumbing', 'alerts'] |  |  |  |  |  |  |  | 0 | 0 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 2 |  | 0 |  |  |  |  |  |  |  | FAIL |

## Log
## 1. Warroom v4 regeneration

## 3. Live page checks (CDN lag = warn-level)

- `22:24:49` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `22:24:49` FAIL: plumbing rows=0 (<18)
- `22:24:49` FAIL: n_votes=250 did not grow past 250
