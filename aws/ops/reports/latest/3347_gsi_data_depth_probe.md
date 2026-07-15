## 1. gsi-dim-history depth & coverage

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-07-15T17:09:57+00:00  

## Log
- `17:09:57` ✅ dim-history: 336 snapshots
- `17:09:57`   span: 2025-03-11 -> 2026-07-15
- `17:09:57`   spy_close present: 279/336
- `17:09:57`   dim 'market': 117/336 non-null (34%)
- `17:09:57`   dim 'credit': 333/336 non-null (99%)
- `17:09:57`   dim 'vix': 332/336 non-null (98%)
- `17:09:57`   dim 'rate_vol': 332/336 non-null (98%)
- `17:09:57`   dim 'contagion': 272/336 non-null (80%)
- `17:09:57`   dim 'sovereign': 266/336 non-null (79%)
- `17:09:57`   first non-null date per dim: {'credit': '2025-03-11', 'vix': '2025-03-11', 'rate_vol': '2025-03-11', 'contagion': '2025-06-11', 'sovereign': '2025-06-20', 'market': '2026-01-23'}
## 2. gsi-calibration.json current state

- `17:09:57`   mode=empirical sample_size=269 snapshots_total=333
- `17:09:57`   ic={'market': -0.1482, 'credit': 0.0336, 'vix': 0.1554, 'rate_vol': -0.3878, 'contagion': -0.3091, 'sovereign': -0.0924}
- `17:09:57`   n_by_dim={'market': 50, 'credit': 269, 'vix': 269, 'rate_vol': 269, 'contagion': 205, 'sovereign': 199}
- `17:09:57`   weights={'market': 0.254, 'credit': 0.1429, 'vix': 0.4, 'rate_vol': 0.1032, 'contagion': 0.05, 'sovereign': 0.05}
## 3. gsi-horizons.json term structure

- `17:09:57`   top keys: ['as_of', 'horizons', 'horizon_labels', 'snapshots_total', 'earliest_snapshot', 'latest_snapshot', 'results', 'dim_dominance', 'term_structure', 'duration_s', 'methodology']
- `17:09:57` ✅ horizons file present
## 4. stress feed inventory (candidate signals beyond 6 dims)

- `17:09:57` ✅ 12/14 candidate stress feeds live
- `17:09:57`   data/ciss-stress.json — 10.0h old
- `17:09:57`   data/eurodollar-stress.json — 19.9h old
- `17:09:57`   data/credit-stress.json — 19.1h old
- `17:09:57`   data/systemic-stress.json — 5.1h old
- `17:09:57`   data/crisis-composite.json — 0.9h old
- `17:09:57`   data/bank-stress.json — 2.1h old
- `17:09:57`   data/vix-curve.json — 0.2h old
- `17:09:57`   data/vvix-vov-regime.json — 19.2h old
- `17:09:57`   data/tail-risk.json — 4.2h old
- `17:09:57`   data/risk-regime.json — 4.4h old
- `17:09:57`   data/global-stress.json — 3.6h old
- `17:09:57`   data/bond-vol.json — 0.9h old
