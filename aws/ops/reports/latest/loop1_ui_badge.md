# Loop 1 UI badge — reports-builder meta + reports.html badge

**Status:** success  
**Duration:** 15.8s  
**Finished:** 2026-04-25T11:31:59+00:00  

## Data

| html_size | rb_zip_size | scorecard_is_meaningful | scorecard_n_calibrated | scorecard_n_with_outcomes |
|---|---|---|---|---|
| 23969 | 15837 | False | 0 | 2 |

## Log
## A. Patch justhodl-reports-builder

- `11:31:43` ✅   Patched reports-builder meta block
- `11:31:43` ✅   Syntax OK
## A2. Re-deploy reports-builder

- `11:31:47` ✅   Re-deployed reports-builder (15,837B)
- `11:31:58` ✅   Invoked in 8.5s
- `11:31:59` 
  scorecard.json meta:
- `11:31:59`     is_meaningful                  False ← NEW
- `11:31:59`     n_calibrated_signals           0 ← NEW
- `11:31:59`     n_signals_with_outcomes        2 ← NEW
- `11:31:59`     has_calibration                True
- `11:31:59`     scored_outcomes                0
## B. Patch reports.html — add Loop 1 calibration badge

- `11:31:59` ✅   Inserted badge CSS
- `11:31:59` ✅   Patched renderHeadline to render calBadge
- `11:31:59`   Wrote reports.html: 23,969B
- `11:31:59`   <div> count: 52, </div> count: 52
## C. Verify integration — scorecard.json + reports.html state

- `11:31:59`   scorecard.json meta.is_meaningful: False
- `11:31:59`   scorecard.json meta.n_calibrated_signals: 0
- `11:31:59`   ⏳ Badge will render YELLOW (Awaiting Data)
- `11:31:59`      Will flip GREEN automatically when ≥30 outcomes are scored
- `11:31:59`      for at least one signal (~May 2 onward).
- `11:31:59` Done
