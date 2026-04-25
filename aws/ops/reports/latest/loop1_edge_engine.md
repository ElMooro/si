# Loop 1 on justhodl-edge-engine (light-touch — no current data, future-proofs)

**Status:** success  
**Duration:** 13.1s  
**Finished:** 2026-04-25T11:31:43+00:00  

## Data

| calibrated_composite | composite | invoke_s | is_meaningful | loop1_installed | zip_size |
|---|---|---|---|---|---|
| 60.0 | 60 | 5.9 | False | True | 14090 |

## Log
## 1. Drop calibration helper into edge-engine source

- `11:31:29` ✅   Wrote: aws/lambdas/justhodl-edge-engine/source/calibration.py (3,653B)
## 2. Patch lambda_function.py

- `11:31:29` ✅   Added calibration import (line ending: '\n')
- `11:31:29` ✅   Patched composite computation to include _loop1_blend + _loop1_meta
- `11:31:29` ✅   Added calibrated_composite + calibration to output dict
- `11:31:29` ✅   Syntax OK
## 3. Re-deploy Lambda

- `11:31:33` ✅   Re-deployed (2 files, 14,090B)
## 4. Sync invoke + verify edge-data.json

- `11:31:42` ✅   Invoked in 5.9s
- `11:31:43` 
  edge-data.json fields:
- `11:31:43`     composite_score           60
- `11:31:43`     calibrated_composite      60.0 ← NEW
- `11:31:43`     raw_composite             60.0 ← NEW
- `11:31:43`     regime                    NEUTRAL
- `11:31:43` 
  Top-level 'calibration' field: ← NEW
- `11:31:43`     is_meaningful: False
- `11:31:43`     n_calibrated:  0
- `11:31:43`     n_signals:     5
- `11:31:43` ✅ 
  ✅ Loop 1 helper installed in justhodl-edge-engine
- `11:31:43`      (calibrated_composite == raw_composite today;
- `11:31:43`       sub-engine signal types not currently scored)
- `11:31:43` Done
