# APPLY - pipeline order pass 2: readers fire after their writers: no reader inside a writer's margin, none within 60 min before it

**Status:** success  
**Duration:** 41.6s  
**Finished:** 2026-10-21T04:22:08+00:00  

## Data

| actions | applied | backups | dry_run | enabled_violators_after | enabled_violators_before | lane_members | late_ticks | max_move_min | moves_by_why | new_ticks | not_as_planned | plan | planned_triggers_live_as_planned | provider_minutes_over_cap | pullers_before_their_print | races_after | races_before | stale_after | stale_before | tick_members_moved |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 16 |  | s3://justhodl-ops-private-857687956942/backups/pipeline-order/20261021T042127Z/ | False |  |  | 12 | 1 | 60 | {"order": 16} | 0 |  | pipeline-order-plan-2.json |  | 0 -> 0 | 0 -> 0 | 0 | 0 | 18 | 32 | 0 |
|  | 16 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | 0 | 0 |  |  |  |  |  | 0 |  | 18 |  |  |  |  |  |  |  |

## Log
## 1. live snapshot

- `04:21:36` bus default: 588 scheduled rules
- `04:21:37` bus justhodl-system-events: 0 scheduled rules
- `04:21:43` scheduler: 435 schedules in 1 group(s)
- `04:21:43` records 1029, enabled triggers able to fire more than once a day: 0
## 2. retimes (changing)

- `04:21:46` retime: 15 items
- `04:21:46` disable: 0 items
## 3. late tick(s)

- `04:21:46` invoke role for new schedules: arn:aws:iam::857687956942:role/justhodl-scheduler-role (template alpha-alerts-sched, may invoke the scheduler: True)
- `04:21:47` late tick daily-morn-late: {"schedule": "jhk-tick-daily-morn-late", "present": true, "live": "cron(15 11 * * ? *)", "live_state": "ENABLED", "live_target_fn": "justhodl-scheduler", "parent_present": true, "parent_expr": "cron(0 11 * * ? *)", "parent_state": "ENABLED", "parent_target_is_scheduler": true, "template_schedule": "alpha-alerts-sched", "role": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "role_may_invoke_scheduler": true}
- `04:21:47` late tick daily-morn-late already in place
## 3b. intraday lane

- `04:21:47` lane schedule jhk-tick-session-5min: absent
- `04:21:47` late tick session-5min: schedule jhk-tick-session-5min ENABLED (created) -> ok
- `04:21:48` control.json cadence_exempt += sched:jhk-tick-session-5min (previous copy backed up)
- `04:21:48` governor cadence exemption: added
## 4. S3 manifests

- `04:21:48` config/fanout-manifest.json: 1 member(s) move to a late tick; result equals the committed repo file
- `04:21:48` config/schedule-manifest.json: 16 entries retimed, 1 schedule entries added; result equals the committed repo file
- `04:21:49` ✅ config/fanout-manifest.json uploaded (previous copy backed up)
- `04:21:50` ✅ config/schedule-manifest.json uploaded (previous copy backed up)
## 5. verification

- `04:22:00` bus default: 588 scheduled rules
- `04:22:00` bus justhodl-system-events: 0 scheduled rules
- `04:22:07` scheduler: 436 schedules in 1 group(s)
- `04:22:08` ✅ pipeline order pass 2 applied: 16 triggers retimed, 1 late tick(s) live, the intraday lane live, cadence floor intact
