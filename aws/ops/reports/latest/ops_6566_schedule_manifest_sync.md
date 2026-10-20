# ops 6566 - deployed schedule manifest == committed manifest

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-10-20T21:37:34+00:00  

## Data

| differences | expected_state_flips | reconciler_mode | uploaded |
|---|---|---|---|
| 8 | 8 | enforce-duplicates | True |

## Log
- `21:37:33` rule ecb-weekly-update: fields state (S3 ENABLED -> repo DISABLED)
- `21:37:33` rule engine-robustness-weekly: fields state (S3 ENABLED -> repo DISABLED)
- `21:37:33` rule justhodl-ici-flows-weekly: fields state (S3 ENABLED -> repo DISABLED)
- `21:37:33` rule justhodl-kill-switch-check: fields state (S3 ENABLED -> repo DISABLED)
- `21:37:33` rule macro-financial-daily-execution: fields state (S3 ENABLED -> repo DISABLED)
- `21:37:33` sched justhodl-hist-banker-weekly: fields state (S3 ENABLED -> repo DISABLED)
- `21:37:33` sched justhodl-sec-bulk-daily: fields state (S3 ENABLED -> repo DISABLED)
- `21:37:33` sched justhodl-transcript-indexer-daily: fields state (S3 ENABLED -> repo DISABLED)
- `21:37:33` reconciler mode (SSM): enforce-duplicates; last drift report: {"generated_at": "2026-10-16T13:29:35.058427+00:00", "mode": "enforce-duplicates", "drift_count": 28, "by_class": {"UNDECLARED": 20, "EXPR_DRIFT": 6, "STATE_DRIFT": 1, "DUPLICATE_TARGET": 1}, "enforced": 1}
- `21:37:34` ✅ S3 copy replaced by the committed manifest (8 state flips from ops 6563 carried over)
