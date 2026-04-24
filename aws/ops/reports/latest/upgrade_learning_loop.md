# Week 1 — Multi-horizon learning loop upgrade

**Status:** success  
**Duration:** 8.2s  
**Finished:** 2026-04-24T23:10:18+00:00  

## Data

| calibrator_default_weights_expanded | new_eb_rules | signal_logger_windows_added |
|---|---|---|
| 10 → 24 | 2 | 5 |

## Log
## FIX 1: Add daily + monthly outcome-checker schedules

- `23:10:11` ✅   Created 'justhodl-outcome-checker-daily' on cron(30 22 ? * MON-FRI *)
- `23:10:11` ✅   Created 'justhodl-outcome-checker-monthly' on cron(0 8 1 * ? *)
## FIX 2: Add 1-day check windows to short-horizon signals

- `23:10:11`   Replaced 5/5 window patterns
- `23:10:11` ✅   signal-logger source valid (10010 bytes), saved
- `23:10:15` ✅   Deployed signal-logger (3,819 bytes)
## FIX 3: Add 8 missing signal types to calibrator's DEFAULT_WEIGHTS

- `23:10:15` ✅   Calibrator source valid (14285 bytes), saved
- `23:10:18` ✅   Deployed calibrator (4,541 bytes)
## FIX 4: Trigger backfill outcome-checker run (async)

- `23:10:18` ✅   Async-triggered outcome-checker (status 202)
- `23:10:18`   This will scan all pending signals and score any whose
- `23:10:18`   windows have elapsed. Should accumulate fresh outcomes
- `23:10:18`   in DynamoDB justhodl-outcomes for next calibration run.
## FIX 5: Verify final outcome-checker EB schedule

- `23:10:18`   Outcome-checker now has 3 schedule(s):
- `23:10:18`     [ENABLED] justhodl-outcome-checker-daily: cron(30 22 ? * MON-FRI *)
- `23:10:18`     [ENABLED] justhodl-outcome-checker-monthly: cron(0 8 1 * ? *)
- `23:10:18`     [ENABLED] justhodl-outcome-checker-weekly: cron(0 8 ? * SUN *)
- `23:10:18` 
  Calibrator schedules: 1
- `23:10:18`     [ENABLED] justhodl-calibrator-weekly: cron(0 9 ? * SUN *)
- `23:10:18` Done
