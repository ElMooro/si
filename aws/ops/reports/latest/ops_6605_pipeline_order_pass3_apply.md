# APPLY - pipeline order pass 3: readers fire after their writers: no reader inside a writer's margin, none within 60 min before it; pullers af

**Status:** success  
**Duration:** 37.3s  
**Finished:** 2026-10-22T01:51:37+00:00  

## Data

| actions | applied | backups | dry_run | enabled_violators_after | enabled_violators_before | lane_members | late_ticks | max_move_min | moves_by_why | new_ticks | not_as_planned | plan | planned_triggers_live_as_planned | provider_minutes_over_cap | pullers_before_their_print | races_after | races_before | stale_after | stale_before | tick_members_moved |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 135 |  | s3://justhodl-ops-private-857687956942/backups/pipeline-order/20261022T015100Z/ | False |  |  | 12 | 2 | 303 | {"burst": 57, "order": 43, "print": 44} | 1 |  | pipeline-order-plan-3.json |  | 36 -> 0 | 51 -> 2 | 0 | 0 | 19 | 22 | 7 |
|  | 135 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | 0 | 0 |  |  |  |  |  | 0 |  | 138 |  |  |  |  |  |  |  |

## Log
## 1. live snapshot

- `01:51:09` bus default: 588 scheduled rules
- `01:51:09` bus justhodl-system-events: 0 scheduled rules
- `01:51:13` scheduler: 438 schedules in 1 group(s)
- `01:51:13` records 1032, enabled triggers able to fire more than once a day: 0
## 2. retimes (changing)

- `01:51:24` retime: 134 items
- `01:51:24` disable: 0 items
## 3. late tick(s)

- `01:51:24` invoke role for new schedules: arn:aws:iam::857687956942:role/justhodl-scheduler-role (template alpha-alerts-sched, may invoke the scheduler: True)
- `01:51:24` late tick daily-morn-late: {"schedule": "jhk-tick-daily-morn-late", "present": true, "live": "cron(15 11 * * ? *)", "live_state": "ENABLED", "live_target_fn": "justhodl-scheduler", "parent_present": true, "parent_expr": "cron(0 11 * * ? *)", "parent_state": "ENABLED", "parent_target_is_scheduler": true, "template_schedule": "alpha-alerts-sched", "role": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "role_may_invoke_scheduler": true}
- `01:51:24` late tick daily-morn-late2: {"schedule": "jhk-tick-daily-morn-late2", "present": false, "live": null, "live_state": null, "live_target_fn": null, "parent_present": true, "parent_expr": "cron(0 11 * * ? *)", "parent_state": "ENABLED", "parent_target_is_scheduler": true, "template_schedule": "alpha-alerts-sched", "role": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "role_may_invoke_scheduler": true}
- `01:51:24` late tick daily-morn-late already in place
- `01:51:24` late tick daily-morn-late2: schedule jhk-tick-daily-morn-late2 ENABLED (created) -> ok
## 3b. intraday lane

- `01:51:24` lane schedule jhk-tick-session-5min: present (cron(0/5 9-16 ? * MON-FRI *) America/New_York ENABLED)
- `01:51:25` control.json cadence_exempt already lists sched:jhk-tick-session-5min
- `01:51:25` governor cadence exemption: already
## 4. S3 manifests

- `01:51:25` config/fanout-manifest.json: 7 member(s) move to a late tick; result equals the committed repo file
- `01:51:25` config/schedule-manifest.json: 135 entries retimed, 1 schedule entries added; result equals the committed repo file
- `01:51:25` ✅ config/fanout-manifest.json uploaded (previous copy backed up)
- `01:51:26` ✅ config/schedule-manifest.json uploaded (previous copy backed up)
## 5. verification

- `01:51:33` bus default: 588 scheduled rules
- `01:51:33` bus justhodl-system-events: 0 scheduled rules
- `01:51:37` scheduler: 439 schedules in 1 group(s)
- `01:51:37` ✅ pipeline order pass 3 applied: 135 triggers retimed, 2 late tick(s) live, the intraday lane live, cadence floor intact
