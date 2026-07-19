# ops 3493 — sector medians republish + vs-Sector surfaces

**Status:** success  
**Duration:** 151.9s  
**Finished:** 2026-07-19T03:06:54+00:00  

## Log
- `03:04:22`   zip: 99137 bytes
## 1. Lambda

- `03:04:23`   Lambda exists — updating
- `03:04:28` ✅   ✓ updated justhodl-fundamental-graphs
- `03:04:32` FAIL  H1_republish — {'n_sectors': 11, 'keys': ['beneish_m', 'fcf_yield_pct', 'pe_ttm', 'peg_ttm', 'ps_ttm', 'sloan_accruals_pct']}
- `03:04:32` PASS  H2_realdata_sanity — {'tech_pe': 34.0, 'util_pe': 22.3, 'beneish_band_ok': True, 'sample': {'Technology': 34.0, 'Healthcare': 25.5, 'Utilities': 22.3, 'Communication Services': 21.9}}
- `03:06:54` PASS  H3_surfaces — {'node_ok': True, 'core_hlines': True, 'flag': True, 'why': True, 'priors': True}
# RESULT: FAILS: ['H1_republish']

