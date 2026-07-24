# ops 3811 — value types of every map the verdict dereferences

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-07-24T17:56:01+00:00  

## Log
## data/estimate-revisions.json -> direction_map

- `17:56:01`   container type: dict  n=393
- `17:56:01`   value types: {'str': 393}
- `17:56:01`   sample: RELL -> FLAT
- `17:56:01` ⚠   ** .get() on these values WILL throw — not dicts **
## data/dark-pool.json -> dark_map

- `17:56:01`   container type: dict  n=939
- `17:56:01`   value types: {'int': 400}
- `17:56:01`   sample: MLM -> 1532511
- `17:56:01` ⚠   ** .get() on these values WILL throw — not dicts **
## data/finra-short.json -> tickers

- `17:56:01`   container type: dict  n=501
- `17:56:01`   value types: {'dict': 400}
- `17:56:01`   sample: SWK -> {'symbol': 'SWK', 'svr': 0.8816, 'svr_pct': 88.16, 'short_volume': 540540.67608, 'total_volume': 613144.264516, 'short_exempt': 26.0, 'svr_5d_avg': 0.6237, 'svr
## all_qualifying / league are lists of dicts (already row-mapped)

- `17:56:01`   all_qualifying                     list n=244 first=dict
- `17:56:01`   league                             list n=119 first=dict
## VERDICT

- `17:56:01`   CULPRIT data/estimate-revisions.json       direction_map -> {'str': 393}
- `17:56:01`   CULPRIT data/dark-pool.json                dark_map -> {'int': 400}
- `17:56:01` ⚠ Fix = guard every map dereference with isinstance(x, dict) before .get(), and skip the leg rather than crashing the whole verdict block. One bad value type currently kills all 2,393 rows.
- `17:56:01` ✅ DIAG.found :: 2 culprit map(s) identified
- `17:56:01` ✅ PASS_ALL — culprit isolated
