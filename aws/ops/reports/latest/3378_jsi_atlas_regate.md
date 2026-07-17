# ops 3378 — JSI atlas invariant re-gate

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-07-17T04:59:42+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:59:42` PASS  G1_coverage — sum_n_1m=9197 vs history_n=9469
- `04:59:42` PASS  G2_quartile_ordering — p25<=med<=p75 across all 40 cells
- `04:59:42` PASS  G3_current_bucket — {"decile": 2, "expanding_pctile": 27.2, "regime_spine": "NORMAL"}
- `04:59:42` PASS  G4_contrarian_recorded — d9 3m med=8.2% pos=64.0% n=724 vs d2 med=3.1%
- `04:59:42` VERDICT: PASS_ALL
