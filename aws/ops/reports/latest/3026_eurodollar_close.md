## 0. Wait for deploys

**Status:** success  
**Duration:** 13.3s  
**Finished:** 2026-07-09T23:00:04+00:00  

## Data

| band | barometer | code_ages_min | dedupe_leaks | eurodollar_firing | eurodollar_rows | funding_family_fallback | funding_green | funding_rows | invoke | mechanisms | n_canaries | n_fails | n_votes | n_warns | note | page_everything_tab | page_gauge | sample | sentinel_informational | sentinel_rows | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | {'justhodl-canary-warroom': 9.2} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | {'ok': True, 'barometer': 36.6, 'master_ew': 23.9, 'n_firing': 108, 'n_divergences': 1} |  |  |  |  |  |  |  |  |  |  |  |  |
| WATCH | 36.6 |  |  |  |  | 0 | 29 | 31 |  |  | 292 |  | 292 |  | Every watched canary = one equal vote of its 0-100 stress — including every individual funding-plumbing member and every |  |  |  | 18 | 52 |  |
|  |  |  | [] | 2 | 20 |  |  |  |  | ['macro_grid', 'funding', 'leading_markets', 'dollar', 'vol', 'ciss', 'factor_regime', 'cftc', 'global_stress', 'plumbing', 'eurodollar', 'alerts'] |  |  |  |  |  |  |  | ['€$ us_core — SOFR 99th pct − IORB (repo tail)', '€$ us_core — EFFR − IORB', '€$ bank_funding — SOFR − 3M T-bill', '€$ bank_funding — OFR FSI — funding stress contribution', '€$ credit — IG credit OAS', '€$ backstops — Fed FIMA repo (foreign-official $ borrowing)'] |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 0 |  | 0 |  |  |  |  |  |  | PASS |

## Log
## 1. Warroom v4 regeneration

## 3. Live page checks (CDN lag = warn-level)

- `23:00:04` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `23:00:04` PASS -- barometer 36.6 (WATCH) over 292 equal votes; full inventory live
