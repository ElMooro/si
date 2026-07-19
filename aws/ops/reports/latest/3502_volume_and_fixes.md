# ops 3502 — eye/auto-TA/macro-dropdown/volume

**Status:** success  
**Duration:** 133.0s  
**Finished:** 2026-07-19T04:48:56+00:00  

## Log
- `04:46:43` FAIL  V0_fmp_volume_probe — 'list' object has no attribute 'get'
- `04:46:43` PASS  V1_volume_battery — {'weekly_sum': True, 'rvol_spike': True, 'cap_sorted': True, 'coverage_block': True, 'short_block': True}
- `04:46:43`   zip: 104759 bytes
## 1. Lambda

- `04:46:44`   Lambda exists — updating
- `04:46:47` ✅   ✓ updated justhodl-fundamental-graphs
- `04:46:53` PASS  V2_aapl_volume_live — {'volume_status': {'state': 'ok', 'coverage_pct': 100.0, 'last': 63407059.0, 'avg20': 66399226.0, 'rvol': 0.95}, 'n_volume_w': 552, 'n_spikes': 8, 'recent_spikes': [['2025-09-19', 'VOL_SPIKE', 'volume spike 3.3x 20d avg'], ['2025-12-19', 'VOL_SPIKE', 'volume spike 3.3x 20d avg'], ['2026-06-26', 'VOL_SPIKE', 'volume spike 4.6x 20d avg']], 'week_reconciliation': None}
- `04:48:56` PASS  V3_surfaces — {'node_ok': True, 'core': True, 'flag': True, 'why': True, 'priors': True}
# RESULT: FAILS: ['V0_fmp_volume_probe']

