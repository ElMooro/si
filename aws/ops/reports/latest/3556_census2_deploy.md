# ops 3556 — ETF + FI census live

**Status:** success  
**Duration:** 144.3s  
**Finished:** 2026-07-20T00:04:50+00:00  

## Log
- `00:02:26`   zip: 86085 bytes
## 1. Lambda

- `00:02:26`   Lambda exists — updating
- `00:02:30` ✅   ✓ updated justhodl-etf-census
- `00:02:33`   zip: 85654 bytes
## 1. Lambda

- `00:02:33`   Lambda missing — creating
- `00:02:38` ✅   ✓ created justhodl-fi-census
- `00:02:38` PASS  A1_zip — {'justhodl-etf-census': True, 'justhodl-fi-census': True}
- `00:03:07` FAIL  A2_etf — {'n': 80, 'beta_n': 80, 'leveraged_n': 9, 'flow_cols': ['f_dvol_5d_vs_20d_pct', 'f_return_1d_pct', 'f_return_20d_pct', 'f_return_5d_pct'], 'tech_top': [('IWD', 93.3), ('VLUE', 89.4), ('IWM', 88.0), ('XBI', 85.4), ('KRE', 84.1), ('QUAL', 82.9)], 'decay_board_n': 9, 'dbl_bottoms': []}
- `00:03:27` PASS  A2_fi — {'n': 45, 'ladder': {'EDV': 1.42, 'TLT': 1.0, 'IEF': 0.47, 'SHY': 0.09, 'BIL': -0.0}, 'ladder_top4': [['ZROZ', 1.53], ['EDV', 1.42], ['TLT', 1.0], ['TLH', 0.82]], 'curve_2s10s_bp': 41.0, 'y10': 4.57, 'hy_oas_bp': 271.0, 'regime': 'CALM'}
- `00:03:27` PASS  A3_sched — 2nd & 16th monthly
- `00:04:50` FAIL  A4_pages — {'node': True, 'pins': False}
