# ops 6584 DRY RUN - pipeline order: readers fire after their writers: no reader inside a writer's margin, none within 60 min before it

**Status:** success  
**Duration:** 16.7s  
**Finished:** 2026-10-21T02:39:07+00:00  

## Data

| actions | already | backups | drift | dry_run | failed | late_tick_problems | late_ticks | max_move_min | mean_input_age_h | missing | races_after | races_before | rules_on_default_bus | stale_after | stale_before | tick_members_moved | would_apply |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 81 |  | s3://justhodl-ops-private-857687956942/backups/pipeline-order/20261021T023851Z/ |  | True |  |  | 1 | 67 | 10.95 -> 9.96 |  | 0 | 20 |  | 31 | 116 | 3 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 81 |
|  | 0 |  | 0 |  | 0 | 0 |  |  |  | 0 |  |  | 590 |  |  |  | 81 |

## Log
## 1. live snapshot

- `02:39:04` bus default: 588 scheduled rules
- `02:39:04` bus justhodl-system-events: 0 scheduled rules
- `02:39:06` scheduler: 434 schedules in 1 group(s)
- `02:39:06` records 1028, enabled triggers able to fire more than once a day: 0
## 2. retimes (would change)

- `02:39:06` retime: 81 items
- `02:39:06` disable: 0 items
## 3. late tick(s)

- `02:39:06` late tick daily-morn-late: {"schedule": "jhk-tick-daily-morn-late", "present": false, "live": null, "live_state": null, "live_target_fn": null, "parent_present": true, "parent_expr": "cron(0 11 * * ? *)", "parent_state": "ENABLED", "parent_target_is_scheduler": true, "template_schedule": "alpha-alerts-sched", "role": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "role_may_invoke_scheduler": true}
## 4. S3 manifests

- `02:39:06` config/fanout-manifest.json: 3 member(s) move to a late tick; result DIFFERS from the committed repo file
- `02:39:07` config/schedule-manifest.json: 81 entries retimed, 1 schedule entries added; result DIFFERS from the committed repo file
## 5. verification

- `02:39:07` ✅ dry run clean: 81 retimes ready, 1 late tick(s) creatable, nothing drifted
