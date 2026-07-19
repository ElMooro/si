# ops 3508 — factor-DNA radar · nightly medians · DuPont

**Status:** success  
**Duration:** 142.2s  
**Finished:** 2026-07-19T06:09:20+00:00  

## Log
- `06:06:58` PASS  H1_factor_battery — {'quality_pct': 98.2, 'expected': 98.2, 'axes': ['quality', 'value', 'momentum', 'growth', 'composite'], 'dormancy': ['ZZZZ not in master-ranker top set', 'ranker rows <30']}
- `06:06:58`   zip: 106775 bytes
## 1. Lambda

- `06:06:58`   Lambda exists — updating
- `06:07:03` ✅   ✓ updated justhodl-fundamental-graphs
- `06:07:13` FAIL  H2_nvda_radar — {'state': 'insufficient', 'why': 'ranker rows <30', 'n_universe': None, 'axes': []}
- `06:07:16` PASS  H3_nightly_medians — {'marker_before_monday_gate': True, 'medians_age_s': 2}
- `06:09:20` PASS  H4_surfaces — {'node': True, 'catalog': True, 'flag': True, 'why': True, 'priors': True}
# RESULT: FAILS: ['H2_nvda_radar']

