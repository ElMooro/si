## 0. Wait for deploys

**Status:** failure  
**Duration:** 33.4s  
**Finished:** 2026-07-09T22:30:19+00:00  

## Error

```
SystemExit: 1
```

## Data

| band | barometer | code_ages_min | dedupe_leaks | funding_family_fallback | funding_green | funding_rows | invoke | live_keys | mechanisms | n_canaries | n_fails | n_votes | n_warns | note | page_everything_tab | page_gauge | plumbing_asof | plumbing_firing | plumbing_n_with_data | plumbing_rows | raw_ind_type | sample | sentinel_informational | sentinel_rows | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | {'justhodl-canary-warroom': 0.0} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | ['alerts', 'as_of', 'composite_label', 'composite_score', 'duration_s', 'layers', 'method', 'n_indicators', 'n_with_data', 'raw_indicators', 'schema_version'] |  |  |  |  |  |  |  |  |  |  |  |  | dict | {'BOGZ1FL663067003Q': ['date', 'err', 'interp', 'label', 'layer', 'percentile', 'polarity', 'source', 'stress_score', 'value', 'yoy_pct', 'z_score']} |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-07-09T22:29:58+00:00 |  | 29 |  |  |  |  |  |  |
|  |  |  |  |  |  |  | {'ok': True, 'barometer': 34.3, 'master_ew': 23.9, 'n_firing': 91, 'n_divergences': 1} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| WATCH | 34.3 |  |  | 0 | 29 | 31 |  |  |  | 250 |  | 250 |  | Every watched canary = one equal vote of its 0-100 stress — including every individual funding-plumbing member and every |  |  |  |  |  |  |  |  | 18 | 52 |  |
|  |  |  | [] |  |  |  |  |  | ['macro_grid', 'funding', 'leading_markets', 'dollar', 'vol', 'ciss', 'factor_regime', 'cftc', 'global_stress', 'plumbing', 'alerts'] |  |  |  |  |  |  |  |  | 0 |  | 0 |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 2 |  | 0 |  |  |  |  |  |  |  |  |  |  |  | FAIL |

## Log
## 0.7 Refresh plumbing aggregator + probe live schema

## 1. Warroom v4 regeneration

## 3. Live page checks (CDN lag = warn-level)

- `22:30:19` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `22:30:19` FAIL: plumbing rows=0 (<18)
- `22:30:19` FAIL: n_votes=250 did not grow past 250
