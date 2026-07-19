# ops 3496 — green/red verdict layer

**Status:** success  
**Duration:** 138.0s  
**Finished:** 2026-07-19T03:22:49+00:00  

## Log
- `03:20:31` PASS  U1_six_suites — {'fortress': True, 'disaster': True, 'fin': True, 'missing': True, 'sector': True, 'rich': True, 'sev3': ['altman_z', 'beneish_m', 'fcf_margin_pct', 'interest_coverage_ttm', 'net_shareholder_yield_pct', 'netdebt_to_ebitda_ttm', 'sloan_accruals_pct']}
- `03:20:31`   zip: 100922 bytes
## 1. Lambda

- `03:20:31`   Lambda exists — updating
- `03:20:34` ✅   ✓ updated justhodl-fundamental-graphs
- `03:20:47` FAIL  U2_aapl_live — 'n_green'
- `03:20:47` FAIL  U3_jpm_suppression — 'fin_suppressed'
- `03:22:48` PASS  U4_surfaces — {'node_ok': True, 'flag': True, 'why': True, 'priors': True}
# RESULT: FAILS: ['U2_aapl_live', 'U3_jpm_suppression']

