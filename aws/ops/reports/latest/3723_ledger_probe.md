# ops 3723 — estimate-revisions ledger probe (obs[0] baselines)

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-07-22T19:47:21+00:00  

## Data

| probe | value |
|---|---|
| ledger_updated | 2026-07-22T19:21:54.161781+00:00 |
| n_keys | 448 |
| obs_length_distribution | {1: 9, 2: 4, 3: 2, 4: 1, 5: 2, 6: 8, 7: 7, 8: 6, 9: 2, 10: 407} |
| baseline_eps | usable=369 null=79 zero=0 (usable_pct=82.4%) |
| latest_eps | usable=369 null=79 |
| keys_with_2plus_obs_and_usable_eps | 363 |
| obs_date_span | 2026-07-03 .. 2026-07-22 (20 distinct days) |
| move_distribution_vs_threshold | {'ZERO': 363} |
| would_pass_threshold | 0 of 363 (REV_THRESHOLD_PCT=1.0) |
| artifact | version=2.1.0 n_tracked=436 n_with_history=436 n_state_keys=448 up=0 down=0 strength_leaders=40 |
| DIAGNOSIS | HEALTHY BUT QUIET — real baselines and real diffs exist, but no move clears REV_THRESHOLD_PCT=1.0. Empty arrays are the CORRECT reading. Consider lowering the threshold or surfacing sub-threshold drift as a separate 'estimate drift' row. |

## Log
- `19:47:20` ledger_updated: 2026-07-22T19:21:54.161781+00:00
- `19:47:20` n_keys: 448
- `19:47:20` obs_length_distribution: {1: 9, 2: 4, 3: 2, 4: 1, 5: 2, 6: 8, 7: 7, 8: 6, 9: 2, 10: 407}
- `19:47:20` baseline_eps: usable=369 null=79 zero=0 (usable_pct=82.4%)
- `19:47:20` latest_eps: usable=369 null=79
- `19:47:20` keys_with_2plus_obs_and_usable_eps: 363
- `19:47:20` obs_date_span: 2026-07-03 .. 2026-07-22 (20 distinct days)
- `19:47:20` move_distribution_vs_threshold: {'ZERO': 363}
- `19:47:20` would_pass_threshold: 0 of 363 (REV_THRESHOLD_PCT=1.0)
- `19:47:21` artifact: version=2.1.0 n_tracked=436 n_with_history=436 n_state_keys=448 up=0 down=0 strength_leaders=40
- `19:47:21` DIAGNOSIS: HEALTHY BUT QUIET — real baselines and real diffs exist, but no move clears REV_THRESHOLD_PCT=1.0. Empty arrays are the CORRECT reading. Consider lowering the threshold or surfacing sub-threshold drift as a separate 'estimate drift' row.
- `19:47:21` VERDICT: DIAGNOSIS COMPLETE
