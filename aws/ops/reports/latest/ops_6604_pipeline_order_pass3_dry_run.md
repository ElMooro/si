# DRY RUN - pipeline order pass 3: readers fire after their writers: no reader inside a writer's margin, none within 60 min before it; pullers af

**Status:** success  
**Duration:** 18.9s  
**Finished:** 2026-10-22T01:44:26+00:00  

## Data

| actions | already | backups | drift | dry_run | failed | lane_members | late_tick_problems | late_ticks | max_move_min | missing | moves_by_why | new_ticks | plan | provider_minutes_over_cap | pullers_before_their_print | races_after | races_before | rules_on_default_bus | stale_after | stale_before | tick_members_moved | would_apply |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 135 |  | s3://justhodl-ops-private-857687956942/backups/pipeline-order/20261022T014407Z/ |  | True |  | 12 |  | 2 | 303 |  | {"burst": 57, "order": 43, "print": 44} | 1 | pipeline-order-plan-3.json | 36 -> 0 | 51 -> 2 | 0 | 0 |  | 19 | 22 | 7 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 135 |
|  | 0 |  | 0 |  | 0 |  | 0 |  |  | 0 |  |  |  |  |  |  |  | 590 |  |  |  | 135 |

## Log
## 1. live snapshot

- `01:44:16` bus default: 588 scheduled rules
- `01:44:16` bus justhodl-system-events: 0 scheduled rules
- `01:44:22` scheduler: 438 schedules in 1 group(s)
- `01:44:22` records 1032, enabled triggers able to fire more than once a day: 0
## 2. retimes (would change)

- `01:44:24` retime: 135 items
- `01:44:24` disable: 0 items
## 3. late tick(s)

- `01:44:24` invoke role for new schedules: arn:aws:iam::857687956942:role/justhodl-scheduler-role (template alpha-alerts-sched, may invoke the scheduler: True)
- `01:44:24` late tick daily-morn-late: {"schedule": "jhk-tick-daily-morn-late", "present": true, "live": "cron(15 11 * * ? *)", "live_state": "ENABLED", "live_target_fn": "justhodl-scheduler", "parent_present": true, "parent_expr": "cron(0 11 * * ? *)", "parent_state": "ENABLED", "parent_target_is_scheduler": true, "template_schedule": "alpha-alerts-sched", "role": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "role_may_invoke_scheduler": true}
- `01:44:25` late tick daily-morn-late2: {"schedule": "jhk-tick-daily-morn-late2", "present": false, "live": null, "live_state": null, "live_target_fn": null, "parent_present": true, "parent_expr": "cron(0 11 * * ? *)", "parent_state": "ENABLED", "parent_target_is_scheduler": true, "template_schedule": "alpha-alerts-sched", "role": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "role_may_invoke_scheduler": true}
## 3b. intraday lane

- `01:44:25` lane schedule jhk-tick-session-5min: present (cron(0/5 9-16 ? * MON-FRI *) America/New_York ENABLED)
- `01:44:25` control.json cadence_exempt already lists sched:jhk-tick-session-5min
- `01:44:25` governor cadence exemption: already
## 4. S3 manifests

- `01:44:25` config/fanout-manifest.json: 7 member(s) move to a late tick; result DIFFERS from the committed repo file
- `01:44:26` config/schedule-manifest.json: 135 entries retimed, 1 schedule entries added; result DIFFERS from the committed repo file
## 5. verification

- `01:44:26` ✅ dry run clean: 135 retimes ready, 2 late tick(s) creatable, nothing drifted
