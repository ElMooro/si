# Fix reports-builder hit_rate calc — only score truly-scored outcomes

**Status:** success  
**Duration:** 14.9s  
**Finished:** 2026-04-25T09:44:15+00:00  

## Data

| rows_with_scored_data | scorecard_rows |
|---|---|
| 0 | 15 |

## Log
## 1. Patch reports-builder source

- `09:44:01` ✅   Patched lambda_function.py
- `09:44:01` ✅   Syntax OK
## 2. Re-deploy reports-builder

- `09:44:04` ✅   Re-deployed (10849B)
## 3. Invoke + verify scorecard.json

- `09:44:15` ✅   Invoked: {'ok': True, 'scorecard_rows': 15, 'timeline_points': 200, 'signals_seen': 4829, 'outcomes_seen': 4377}
- `09:44:15`   Scorecard rows: 15
- `09:44:15`   Sample (top 5 by total):
- `09:44:15`     screener_top_pick         total= 940  scored=   0  hit_rate=None
- `09:44:15`     crypto_fear_greed         total= 442  scored=   0  hit_rate=None
- `09:44:15`     crypto_risk_score         total= 442  scored=   0  hit_rate=None
- `09:44:15`     khalid_index              total= 334  scored=   0  hit_rate=None
- `09:44:15`     ml_risk                   total= 334  scored=   0  hit_rate=None
- `09:44:15` Done
