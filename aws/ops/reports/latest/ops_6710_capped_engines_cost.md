# ops 6710 - the CAP-held engines: why, triggers, and their monthly Lambda cost (read-only)

**Status:** success  
**Duration:** 7.9s  
**Finished:** 2026-11-02T16:05:15+00:00  

## Log
## A. governor ledger

- `16:05:08` engine cap state: {"history": [{"at": "2026-10-20T19:42:48.856487+00:00", "enforce": true, "functions": ["justhodl-miss-calibrator", "justhodl-event-coordinator", "justhodl-spend-governor-canary"]}], "released": {}}
- `16:05:08` justhodl-event-coordinator: {"applied_at": "2026-10-17T01:55:44.912478+00:00", "before": {"reserved": 10}, "cap_usd": 50.0, "capped_at": "2026-10-20T19:42:48.856487+00:00", "capped_from": "FREEZE", "drift_seen_at": "2026-10-17T01:55:44.912478+00:00", "id": "justhodl-event-coordinator", "key": "lambda:justhodl-event-coordinator", "lever": "lambda", "restorable": true, "stage": "CAP", "status": "reapplied", "usd_at_cap": 431.7289}
- `16:05:08` justhodl-miss-calibrator: {"applied_at": "2026-10-17T01:55:44.912478+00:00", "before": {"reserved": 1}, "cap_usd": 50.0, "capped_at": "2026-10-20T19:42:48.856487+00:00", "capped_from": "FREEZE", "drift_seen_at": "2026-10-17T01:55:44.912478+00:00", "id": "justhodl-miss-calibrator", "key": "lambda:justhodl-miss-calibrator", "lever": "lambda", "restorable": true, "stage": "CAP", "status": "reapplied", "usd_at_cap": 1551.2815}
- `16:05:08` macro-financial-intelligence: {"applied_at": "2026-10-16T22:53:03.699511+00:00", "before": {"reserved": null}, "capped_at": "2026-10-20T20:55:53.109492+00:00", "capped_from": "FREEZE", "id": "macro-financial-intelligence", "key": "lambda:macro-financial-intelligence", "lever": "lambda", "operator_hold": true, "reason": "retired 2026-10-20 (ops 6563): Khalid, 2026-10-20: switch off the orphans and the broken writers; delete the id", "restorable": true, "stage": "CAP", "status": "applied"}
- `16:05:08` ofrapi: {"applied_at": "2026-10-16T22:53:03.699511+00:00", "before": {"reserved": null}, "capped_at": "2026-10-20T20:55:54.358869+00:00", "capped_from": "FREEZE", "id": "ofrapi", "key": "lambda:ofrapi", "lever": "lambda", "operator_hold": true, "reason": "retired 2026-10-20 (ops 6563): Khalid, 2026-10-20: switch off the orphans and the broken writers; delete the id", "restorable": true, "stage": "CAP", "status": "applied"}
- `16:05:08` treasury-api: {"applied_at": "2026-10-16T22:53:03.699511+00:00", "before": {"reserved": null}, "capped_at": "2026-10-20T20:55:54.785648+00:00", "capped_from": "FREEZE", "id": "treasury-api", "key": "lambda:treasury-api", "lever": "lambda", "operator_hold": true, "reason": "retired 2026-10-20 (ops 6563): Khalid, 2026-10-20: switch off the orphans and the broken writers; delete the id", "restorable": true, "stage": "CAP", "status": "applied"}
- `16:05:08` state.alerts = {"risk_day": "2026-11-01"}
- `16:05:08` state.cap_at_stage = null
- `16:05:08` state.caps = {"history": [{"at": "2026-10-20T19:42:48.856487+00:00", "enforce": true, "functions": ["justhodl-miss-calibrator", "justhodl-event-coordinator", "justhodl-spend-governor-canary"]}], "released": {}}
- `16:05:08` state.fast_fuse = {"alarm": "justhodl-runaway-invocations", "at": "2026-10-17T02:00:50.936915+00:00", "buckets": {}, "fleet_usd_day": 0.08, "hot": [], "new": [], "picks": [], "top": [["justhodl-spend-governor", 0.06], ["justhodl-backend-agent", 0.01], ["justhodl-a2a-bus", 0.0], ["justhodl-guardrail-notify", 0.0], ["j
- `16:05:08` state.guardrails = {"checked_at": "2026-11-02T15:07:37.713574+00:00", "problems": []}
- `16:05:08` state.month = "2026-11"
- `16:05:08` state.observations = [["2026-11-01T00:07:37.791709+00:00", 1176.94], ["2026-11-01T01:07:37.805342+00:00", 1176.94], ["2026-11-01T02:07:37.860579+00:00", 1176.94], ["2026-11-01T03:07:37.804400+00:00", 1176.94], ["2026-11-01T04:07:37.724111+00:00", 1176.94], ["2026-11-01T05:07:37.889707+00:00", 1176.94], ["2026-11-01T06:0
- `16:05:08` state.stage = "NORMAL"
- `16:05:08` state.stage_since = "2026-11-02T15:44:47.324976+00:00"
- `16:05:08` state.suppress_until = null
- `16:05:08` state.updated_at = "2026-11-02T15:07:37.713574+00:00"
- `16:05:08` state.version = "1.5.3"
## B. configuration and every trigger that would fire it once released

- `16:05:09` justhodl-event-coordinator: 256 MB x86_64, timeout 60 s, runtime python3.12, modified 2026-10-17T01:53, reserved concurrency 0 | Central event router. Subscribes to justhodl-system-events bus, translates events into Lambda invokes plus Telegram alerts plus audit log.
- `16:05:09`    rules none | schedules none | event sources none | function URL None | invoke grants ['{"Service": "events.amazonaws.com"}<-rule/justhodl-system-events/justhodl-events-to-coordinator']
- `16:05:10` justhodl-miss-calibrator: 512 MB x86_64, timeout 300 s, runtime python3.12, modified 2026-10-16T19:54, reserved concurrency 0 | Closed-loop self-improvement. Reads 30d miss-summary, proposes threshold adjustments to SSM. Final link in the calibrator chain.
- `16:05:10`    rules ['miss-calibrator-weekly [ENABLED] cron(15 9 ? * MON *)'] | schedules none | event sources none | function URL None | invoke grants ['{"Service": "events.amazonaws.com"}<-rule/miss-calibrator-weekly']
- `16:05:10` macro-financial-intelligence: 512 MB arm64, timeout 900 s, runtime python3.11, modified 2026-08-02T20:41, reserved concurrency 0 | JustHodl.AI Lambda
- `16:05:10`    rules ['macro-financial-daily-execution [DISABLED] rate(1 day)'] | schedules none | event sources none | function URL https://qhmg6ybf5qhkviw2c6tlizar4e0xhbrc.lambda-url.us-east-1.on.aws/ | invoke grants ['"*"<-', '{"Service": "events.amazonaws.com"}<-rule/macro-financial-daily-execution']
- `16:05:11` ofrapi: 512 MB arm64, timeout 120 s, runtime python3.12, modified 2026-10-08T16:19, reserved concurrency 0 | 
- `16:05:11`    rules none | schedules none | event sources none | function URL None | invoke grants ['{"Service": "apigateway.amazonaws.com"}<-6nl5fzfus7/*/*', '{"Service": "apigateway.amazonaws.com"}<-6nl5fzfus7/*/*/*', '{"Service": "events.amazonaws.com"}<-rule/ofr-daily-collection', '{"Service": "events.amazonaws.com"}<-rule/ofr-weekly-report']
- `16:05:12` treasury-api: 512 MB arm64, timeout 60 s, runtime python3.12, modified 2026-10-08T16:20, reserved concurrency 0 | 
- `16:05:12`    rules none | schedules none | event sources none | function URL https://oanydg4qltq5emsnnb2m23mifm0bqjqh.lambda-url.us-east-1.on.aws/ | invoke grants ['"*"<-', '{"Service": "apigateway.amazonaws.com"}', '{"Service": "apigateway.amazonaws.com"}<-i1hgpjotq7/*/*', '{"Service": "apigateway.amazonaws.com"}<-i1hgpjotq7/*/*/*', '{"Service": "apigateway.amazonaws.com"}<-klehdyiwrl/*/*', '{"Service": "events.amazonaws.com"}<-rule/treasury-auto-update']
## C. Lambda cost by month and by day (CloudWatch, governor formula)

- `16:05:13` justhodl-event-coordinator (256 MB x86_64): 2026-07 $0.01 2407 inv 0 err 0 throttled (31 active days) | 2026-08 $0.00 1418 inv 0 err 0 throttled (31 active days) | 2026-09 $0.01 15116 inv 0 err 0 throttled (30 active days) | 2026-10 $431.73 20973258 inv 11 err 337062 throttled (16 active days)
- `16:05:13`    normal window 2026-09-01..2026-10-11: 41 active days, median $0.0002/active day, mean $0.0002/day -> ~$0.01/month; median 479 inv/day, avg 0.1 s/inv
- `16:05:13`    daily since Oct 1: 10-01 $0.00/492 inv/0 err, 10-02 $0.00/479 inv/0 err, 10-03 $0.00/463 inv/0 err, 10-04 $0.00/48 inv/0 err, 10-05 $0.00/106 inv/0 err, 10-06 $0.00/433 inv/0 err, 10-07 $0.00/420 inv/0 err, 10-08 $0.00/463 inv/0 err, 10-09 $0.00/450 inv/0 err, 10-10 $0.00/475 inv/0 err, 10-11 $0.00/64 inv/0 err, 10-12 $53.17/3082441 inv/0 err, 10-13 $104.96/4525179 inv/6 err, 10-14 $98.52/4692506 inv/1 err, 10-15 $98.90/4679621 inv/0 err, 10-16 $76.18/3989618 inv/4 err
- `16:05:13` justhodl-miss-calibrator (512 MB x86_64): 2026-07 $0.00 108 inv 0 err 0 throttled (5 active days) | 2026-08 $0.00 116 inv 0 err 0 throttled (6 active days) | 2026-09 $0.00 69 inv 0 err 0 throttled (4 active days) | 2026-10 $1551.28 12115587 inv 2071 err 197777 throttled (6 active days)
- `16:05:13`    normal window 2026-09-01..2026-10-11: 5 active days, median $0.0001/active day, mean $0.0000/day -> ~$0.00/month; median 16 inv/day, avg 1.2 s/inv
- `16:05:13`    daily since Oct 1: 10-05 $0.00/12 inv/0 err, 10-12 $228.00/1779920 inv/282 err, 10-13 $334.10/2611486 inv/340 err, 10-14 $347.19/2711595 inv/544 err, 10-15 $346.33/2704800 inv/488 err, 10-16 $295.66/2307774 inv/417 err
- `16:05:13` macro-financial-intelligence (512 MB arm64): 2026-07 $0.01 353 inv 0 err 26 throttled (31 active days) | 2026-08 $0.00 241 inv 0 err 0 throttled (31 active days) | 2026-09 $0.00 469 inv 0 err 0 throttled (30 active days) | 2026-10 $0.00 249 inv 0 err 72 throttled (15 active days)
- `16:05:13`    normal window 2026-09-01..2026-10-11: 41 active days, median $0.0000/active day, mean $0.0000/day -> ~$0.00/month; median 12 inv/day, avg 0.2 s/inv
- `16:05:13`    daily since Oct 1: 10-01 $0.00/8 inv/0 err, 10-02 $0.00/12 inv/0 err, 10-03 $0.00/12 inv/0 err, 10-04 $0.00/12 inv/0 err, 10-05 $0.00/12 inv/0 err, 10-06 $0.00/15 inv/0 err, 10-07 $0.00/13 inv/0 err, 10-08 $0.00/57 inv/0 err, 10-09 $0.00/5 inv/0 err, 10-10 $0.00/81 inv/0 err, 10-11 $0.00/10 inv/0 err, 10-12 $0.00/5 inv/0 err, 10-13 $0.00/3 inv/0 err, 10-15 $0.00/2 inv/0 err, 10-16 $0.00/2 inv/0 err
- `16:05:13` ofrapi (512 MB arm64): 2026-07 $0.01 1920 inv 0 err 0 throttled (31 active days) | 2026-08 $0.00 1 inv 0 err 0 throttled (1 active days) | 2026-09 $0.00 30 inv 0 err 0 throttled (29 active days) | 2026-10 $0.00 13 inv 0 err 0 throttled (13 active days)
- `16:05:13`    normal window 2026-09-01..2026-10-11: 40 active days, median $0.0000/active day, mean $0.0000/day -> ~$0.00/month; median 1 inv/day, avg 0.0 s/inv
- `16:05:13`    daily since Oct 1: 10-01 $0.00/1 inv/0 err, 10-02 $0.00/1 inv/0 err, 10-03 $0.00/1 inv/0 err, 10-04 $0.00/1 inv/0 err, 10-05 $0.00/1 inv/0 err, 10-06 $0.00/1 inv/0 err, 10-07 $0.00/1 inv/0 err, 10-08 $0.00/1 inv/0 err, 10-09 $0.00/1 inv/0 err, 10-10 $0.00/1 inv/0 err, 10-11 $0.00/1 inv/0 err, 10-12 $0.00/1 inv/0 err, 10-13 $0.00/1 inv/0 err
- `16:05:13` treasury-api (512 MB arm64): 2026-07 $0.00 520 inv 0 err 6 throttled (31 active days) | 2026-08 $0.00 901 inv 0 err 0 throttled (31 active days) | 2026-09 $0.00 1279 inv 0 err 0 throttled (30 active days) | 2026-10 $0.00 650 inv 0 err 192 throttled (16 active days)
- `16:05:13`    normal window 2026-09-01..2026-10-11: 41 active days, median $0.0000/active day, mean $0.0000/day -> ~$0.00/month; median 39 inv/day, avg 0.0 s/inv
- `16:05:13`    daily since Oct 1: 10-01 $0.00/35 inv/0 err, 10-02 $0.00/39 inv/0 err, 10-03 $0.00/39 inv/0 err, 10-04 $0.00/39 inv/0 err, 10-05 $0.00/39 inv/0 err, 10-06 $0.00/42 inv/0 err, 10-07 $0.00/40 inv/0 err, 10-08 $0.00/84 inv/0 err, 10-09 $0.00/32 inv/0 err, 10-10 $0.00/108 inv/0 err, 10-11 $0.00/37 inv/0 err, 10-12 $0.00/26 inv/0 err, 10-13 $0.00/25 inv/0 err, 10-14 $0.00/21 inv/0 err, 10-15 $0.00/16 inv/0 err, 10-16 $0.00/28 inv/0 err
## D. what the coordinator drives: its routed targets, cost while it routed vs while it was held

- `16:05:13` routes 33, invoke targets 7: ['justhodl-alpha-calibrator', 'justhodl-alpha-compass', 'justhodl-calibrator', 'justhodl-engine-signal-map', 'justhodl-jh-fusion', 'justhodl-miss-calibrator', 'justhodl-signal-board']
- `16:05:15`    justhodl-alpha-calibrator (1024 MB): routed era $0.000/day 0.9 inv/day | held era $0.000/day 0.1 inv/day
- `16:05:15`    justhodl-alpha-compass (512 MB): routed era $0.001/day 33.8 inv/day | held era $0.000/day 1.1 inv/day
- `16:05:15`    justhodl-calibrator (2048 MB): routed era $0.002/day 0.9 inv/day | held era $0.001/day 1.0 inv/day
- `16:05:15`    justhodl-engine-signal-map (512 MB): routed era $0.000/day 2.5 inv/day | held era $0.000/day 1.0 inv/day
- `16:05:15`    justhodl-jh-fusion (1024 MB): routed era $0.001/day 21.1 inv/day | held era $0.000/day 1.0 inv/day
- `16:05:15`    justhodl-miss-calibrator (512 MB): routed era $0.000/day 2.0 inv/day | held era $0.000/day 0.0 inv/day
- `16:05:15`    justhodl-signal-board (1024 MB): routed era $0.001/day 4.4 inv/day | held era $0.001/day 1.0 inv/day
- `16:05:15`    targets together: $0.01/day while routing vs $0.00/day while held -> coordinator-driven ~$0.10/month
- `16:05:15` ✅ ops 6710 complete (read-only)
