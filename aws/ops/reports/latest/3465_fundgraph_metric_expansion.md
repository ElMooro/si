# ops 3465 — 200-metric library + Favorites/Institutional picker

**Status:** success  
**Duration:** 217.2s  
**Finished:** 2026-07-18T19:37:11+00:00  

## Log
- `19:33:34`   zip: 91818 bytes
## 1. Lambda

- `19:33:35`   Lambda exists — updating
- `19:33:38` ✅   ✓ updated justhodl-fundamental-graphs
- `19:33:54` PASS  M1_deploy_warm_v110 — {'version': '1.1.0', 'warmed': {'CHTR_quarter': {'ok': True, 'n': 44, 'keys': 197}, 'CHTR_annual': {'ok': True, 'n': 12, 'keys': 197}, 'AAPL_quarter': {'ok': True, 'n': 44, 'keys': 200}, 'AAPL_annual': {'ok': True, 'n': 12, 'keys': 200}, 'MSFT_quarter': {'ok': True, 'n': 44, 'keys': 200}, 'MSFT_annual': {'ok': True, 'n': 12, 'keys': 200}}}
- `19:33:55` PASS  M2_hard_institutional_set — {'catalog_n': 200, 'missing': [], 'thin': [], 'rule_of_40_last': ['2026-03-28', 41.371], 'roic_last': ['2026-03-28', 99.697]}
- `19:33:55` PASS  M3_soft_coverage_log — {'employees': 44, 'revenue_per_employee': 44, 'peg_ttm': 32, 'kz_index': 42, 'fulmer_h': 37, 'rule_of_40': 43, 'dps_yoy_pct': 43, 'retention_pct': 44}
- `19:37:11` PASS  M4_page_v12_live — {'status': 200, 'markers': True}
# RESULT: ALL PASS

