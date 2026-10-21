# DRY RUN - pipeline order pass 2: readers fire after their writers: no reader inside a writer's margin, none within 60 min before it

**Status:** success  
**Duration:** 14.4s  
**Finished:** 2026-10-21T04:20:18+00:00  

## Data

| actions | already | backups | drift | dry_run | failed | lane_members | late_tick_problems | late_ticks | max_move_min | missing | moves_by_why | new_ticks | plan | provider_minutes_over_cap | pullers_before_their_print | races_after | races_before | rules_on_default_bus | stale_after | stale_before | tick_members_moved | would_apply |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 16 |  | s3://justhodl-ops-private-857687956942/backups/pipeline-order/20261021T042004Z/ |  | True |  | 12 |  | 1 | 60 |  | {"order": 16} | 0 | pipeline-order-plan-2.json | 0 -> 0 | 0 -> 0 | 0 | 0 |  | 18 | 32 | 0 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 16 |
|  | 0 |  | 0 |  | 0 |  | 0 |  |  | 0 |  |  |  |  |  |  |  | 590 |  |  |  | 16 |

## Log
## 1. live snapshot

- `04:20:14` bus default: 588 scheduled rules
- `04:20:14` bus justhodl-system-events: 0 scheduled rules
- `04:20:16` scheduler: 435 schedules in 1 group(s)
- `04:20:16` records 1029, enabled triggers able to fire more than once a day: 0
## 2. retimes (would change)

- `04:20:16` retime: 16 items
- `04:20:16` disable: 0 items
## 3. late tick(s)

- `04:20:17` invoke role for new schedules: arn:aws:iam::857687956942:role/justhodl-scheduler-role (template alpha-alerts-sched, may invoke the scheduler: True)
- `04:20:17` late tick daily-morn-late: {"schedule": "jhk-tick-daily-morn-late", "present": true, "live": "cron(15 11 * * ? *)", "live_state": "ENABLED", "live_target_fn": "justhodl-scheduler", "parent_present": true, "parent_expr": "cron(0 11 * * ? *)", "parent_state": "ENABLED", "parent_target_is_scheduler": true, "template_schedule": "alpha-alerts-sched", "role": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "role_may_invoke_scheduler": true}
## 3b. intraday lane

- `04:20:17` lane schedule jhk-tick-session-5min: absent
- `04:20:17` control.json cadence_exempt would get sched:jhk-tick-session-5min (now: [])
- `04:20:17` governor cadence exemption: would_add
## 4. S3 manifests

- `04:20:17` config/fanout-manifest.json: 1 member(s) move to a late tick; result DIFFERS from the committed repo file
- `04:20:17` config/schedule-manifest.json: 16 entries retimed, 1 schedule entries added; result DIFFERS from the committed repo file
## 5. verification

- `04:20:18` ✅ dry run clean: 16 retimes ready, 1 late tick(s) creatable, nothing drifted
