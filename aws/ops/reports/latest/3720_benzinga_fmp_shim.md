# ops 3720 — shared/benzinga.py FMP calendar fallback

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-07-22T19:17:39+00:00  

## Data

| detail | gate | ok |
|---|---|---|
| rows=4000 with_epsEstimated=2868 keys=['date', 'epsActual', 'epsEstimated', 'lastUpdated', 'revenueActual', 'revenueEstimated', 'symbol'] | P1_fmp_probe | True |
| fetch_calendar now falls back to FMP when Massive yields nothing | G1_shim_added | True |
| shared/benzinga.py compiles | G2_module_compiles | True |
| repaired by this shim: ['justhodl-earnings-tracker', 'justhodl-estimate-revisions'] | G3_consumers_identified | True |

## Log
## probe — is FMP earnings-calendar usable?

- `19:17:39` P1_fmp_probe True
## patch — shared/benzinga.py

- `19:17:39` G1_shim_added True
- `19:17:39` G2_module_compiles True
- `19:17:39` G3_consumers_identified True
- `19:17:39` VERDICT: PASS_ALL
