# Loop 1A — build shared calibration helper module

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-04-25T10:47:23+00:00  

## Data

| helper_loc | helper_size_b | self_tests_passed |
|---|---|---|
| 209 | 7260 | 4 |

## Log
- `10:47:23` ✅   Wrote canonical: aws/shared/calibration.py (7,260B, 209 LOC)
- `10:47:23` ✅   Syntax OK
## Self-test with synthetic calibration

- `10:47:23`   Test 1: 3 signals all = 70
- `10:47:23`     raw_value (uniform): 70.00  (would be 70 with no calibration)
- `10:47:23`     weighted value: 70.00  (high-trust signal dominates)
- `10:47:23`     n_calibrated: 3/3
- `10:47:23`       khalid_index         score=70  w=1.50  contrib=105.0  calibrated=True
- `10:47:23`       edge_composite       score=70  w=0.50  contrib=35.0  calibrated=True
- `10:47:23`       crypto_fear_greed    score=70  w=1.00  contrib=70.0  calibrated=True
- `10:47:23`   Test 2: high-trust=80, low-trust=20
- `10:47:23`     raw_value: 50.00 (uniform avg = 50)
- `10:47:23`     weighted value: 65.00  (should lean bullish)
- `10:47:23` ✅     weighted (65.0) > raw (50.0) ✓ leans toward calibrated signal
- `10:47:23`   Test 3: empty calibration → uniform weights
- `10:47:23`     raw: 70.0, weighted: 70.0, n_calibrated: 0
- `10:47:23` ✅     weighted == raw (both 70.0) ✓ safe fallback
- `10:47:23`   Test 4: signal with weight but no accuracy → weight = 1.0 (should be 1.0)
- `10:47:23` ✅     falls back to 1.0 when accuracy data missing ✓
- `10:47:23` Done
