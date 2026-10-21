# ops 6585 apply - pipeline order: readers fire after their writers: no reader inside a writer's margin, none within 60 min before it

**Status:** success  
**Duration:** 59.6s  
**Finished:** 2026-10-21T02:59:03+00:00  

## Data

| actions | applied | backups | dry_run | enabled_violators_after | enabled_violators_before | late_ticks | max_move_min | mean_input_age_h | not_as_planned | planned_triggers_live_as_planned | races_after | races_before | stale_after | stale_before | tick_members_moved |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 82 |  | s3://justhodl-ops-private-857687956942/backups/pipeline-order/20261021T025804Z/ | False |  |  | 1 | 67 | 10.95 -> 9.96 |  |  | 0 | 22 | 32 | 116 | 3 |
|  | 82 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | 0 | 0 |  |  |  | 0 | 83 |  |  |  |  |  |

## Log
## 1. live snapshot

- `02:58:13` bus default: 588 scheduled rules
- `02:58:13` bus justhodl-system-events: 0 scheduled rules
- `02:58:20` scheduler: 434 schedules in 1 group(s)
- `02:58:20` records 1028, enabled triggers able to fire more than once a day: 0
## 2. retimes (changing)

- `02:58:31` retime: 81 items
- `02:58:31` disable: 0 items
## 3. late tick(s)

- `02:58:43` late tick daily-morn-late: {"schedule": "jhk-tick-daily-morn-late", "present": false, "live": null, "live_state": null, "live_target_fn": null, "parent_present": true, "parent_expr": "cron(0 11 * * ? *)", "parent_state": "ENABLED", "parent_target_is_scheduler": true, "template_schedule": "alpha-alerts-sched", "role": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "role_may_invoke_scheduler": true}
- `02:58:44` late tick daily-morn-late: schedule jhk-tick-daily-morn-late ENABLED (created) -> ok
## 4. S3 manifests

- `02:58:44` config/fanout-manifest.json: 3 member(s) move to a late tick; result equals the committed repo file
- `02:58:44` config/schedule-manifest.json: 82 entries retimed, 1 schedule entries added; result equals the committed repo file
- `02:58:45` ✅ config/fanout-manifest.json uploaded (previous copy backed up)
- `02:58:46` ✅ config/schedule-manifest.json uploaded (previous copy backed up)
## 5. verification

- `02:58:55` bus default: 588 scheduled rules
- `02:58:56` bus justhodl-system-events: 0 scheduled rules
- `02:59:02` scheduler: 435 schedules in 1 group(s)
- `02:59:03` ✅ pipeline order applied: 82 triggers retimed, 1 late tick(s) live, cadence floor intact
