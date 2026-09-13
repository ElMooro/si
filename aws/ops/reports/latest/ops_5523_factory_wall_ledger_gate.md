# ops 5523 -- factory audit fixes: wall ledger, grader quota, verifier isolation, snapshot retention

**Status:** success  
**Duration:** 80.7s  
**Finished:** 2026-09-13T19:39:38+00:00  

## Data

| elapsed_seconds | errors | gear_b_status | head | holdout_code_ids | holdout_frozen_at | snapshot_ceiling_after_rule | snapshot_objects_now | tick_status | verified_code_rows | wall |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | 7f20e60517 |  |  |  |  |  |  |  |
|  |  |  |  |  |  | ~4,320 (3 days x 1,440) | 582 |  |  |  |
| 0.876 | [] |  |  |  |  |  |  | running |  | {"entries": 0, "final_weeks": 0, "graded": 0, "grader_calls": 0, "independent_weeks": 0, "last_checked_at": "2026-09-13T19:35:33+00:00", "new_events": 0, "official_data_status": "runner_written_prints_required", "pass_complete": true, "pending": 0} |
|  |  | {"version": "gear-b.1", "status": "off", "refusal": "factory/control/gearb.json is absent -- Gear B is off until an owner ops script writes it", "enabled": false, "model_card": "huggingface-llm-qwen2-5-coder-7b-instruct", "instance_type": "ml.g5.2xlarge", "budget": {"daily_usd": 0.0, "season_cap_usd": 0.0, "committed_today_usd": 0.0, "committed_season_usd": null}, "holdout": {"frozen": false, "fro |  | 0 | None |  |  |  | 0 |  |

## Log
- `19:38:18` ✅ justhodl-factory-grader: receipt commit 6d1617d3ac run 34776809605
- `19:38:18` ✅ justhodl-student-rsi: receipt commit 6d1617d3ac run 34776809605
- `19:38:18` ✅ justhodl-ai: receipt commit 6d1617d3ac run 34776809605
- `19:38:18` ✅ student role justhodl-student-rsi-role/factory-gear-a: prints read + ledger write widened (3 changes)
- `19:38:19` ✅ lifecycle jh-factory-runtime-snapshots-3d: expire factory/runtime/snapshots/ after 3 days (2 other rules kept)
- `19:38:19` ✅ waiting 75 s for IAM propagation before the live tick
- `19:39:36` ✅ tick_ok=True
- `19:39:36` ✅ no_wall_errors=True
- `19:39:36` ✅ elapsed_lt_40s=True
- `19:39:36` ✅ wall_pass_complete=True
- `19:39:36` ✅ state_version_advanced=True
- `19:39:36` ✅ ledger_present_or_empty_season=True
- `19:39:38` ✅ grader probe status=rejected reason=prediction_missing quota 1 -> 1 (not consumed)
- `19:39:38` ✅ Gear B untouched by this op: no job launched, no budget changed; a job needs >= 1,500 verified rows
- `19:39:38` ✅ GREEN -- wall ledger live, grader polls free, verifier unprivileged, snapshots capped
