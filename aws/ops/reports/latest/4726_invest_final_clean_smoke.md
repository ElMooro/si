# ops 4726 — final clean smoke test

**Status:** success  
**Duration:** 1.8s  
**Finished:** 2026-08-15T21:50:31+00:00  

## Data

| days_until_zscores_possible | function_error | has_debug_key | history_days_accrued | invoke_elapsed_s | schema | status_code |
|---|---|---|---|---|---|---|
|  | None |  |  | 1.7 |  | 200 |
|  |  | False |  |  | invest/0.1 |  |
| 7 |  |  | 1 |  |  |  |

## Log
- `21:50:31` ✅   invoke succeeded in 1.7s: {"ok": true, "confirmed": 0, "gates_pass": 0, "picks": 0}
- `21:50:31` ✅   clean output, no debug scaffolding
- `21:50:31` ✅   1 day(s) of leg-history accrued so far -- 7 more scheduled runs until z-scores (and possible CONFIRMED/TURNING verdicts) can appear
## Verdict

- `21:50:31` ✅ Clean, deployed, scheduled daily 15:00 UTC, no crash, no debug leakage. Bootstrap period confirmed and will resolve itself as the daily schedule accrues history -- nothing further to do here.
