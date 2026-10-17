# Runaway guardrails - how a loop or storm is stopped before it costs money

Built 2026-10-17 after the miss-calibrator / event-coordinator loop (Oct 12-16, ~$2,300;
`docs/ops/POSTMORTEM_2026-10_EVENT_LOOP.md`). Five independent layers. Any one of them would have stopped
that incident; together they cover loops the event bus never sees.

| # | Layer | Stops | Time to act | Where |
|---|---|---|---|---|
| 1 | **Deploy gate** | a loop being shipped at all | before deploy | `scripts/event_graph.py`, `tests/deployment/test_event_graph_acyclic.py` |
| 2 | **Circuit breaker** | a loop through the event coordinator | at the budget (6-90 runs per target-hour) | `justhodl-event-coordinator`, DynamoDB `justhodl-event-breaker` |
| 3 | **Blast-radius caps** | a loop eating the shared Lambda pool | instantly | reserved concurrency + 15-min async event age on the coordinator, its targets and the a2a pair |
| 4 | **Fast fuse** | any sustained runaway, any path | ~20 min | alarms `justhodl-runaway-*` -> SNS -> `justhodl-spend-governor` v1.1.0 |
| 5 | **Money backstops** | anything else (any service) | hours to a day | hourly governor fuse + $500 cap, daily $40 budget, Cost Anomaly e-mails, bucket-size alarm, versioned-bucket bound |

## What each layer would have done on 2026-10-12

* **Gate:** the pre-fix calibrator source fails it:
  `EVENT LOOP: justhodl-miss-calibrator -[event:near_miss.extreme]-> justhodl-miss-calibrator`. 42bedb1c would
  never have been deployable on top of that route.
* **Breaker:** miss-calibrator's budget is 6 runs per hour; the 7th routed event that hour is acknowledged and
  skipped, so the chain dies inside the first minute (`tests/test_event_coordinator_breaker.py::test_storm_is_bounded`:
  1,000 deliveries start at most 6 runs).
* **Caps:** coordinator <= 10 concurrent, miss-calibrator <= 1: the loop could hold 11 slots, not 764.
* **Fast fuse:** the storm held 760 concurrent / 35,302 invocations / 227M ms per 5 min; all three alarms fire 15
  minutes in, the governor throttles both functions in the same invocation. Cost at most ~$6.
* **Backstops:** the old `justhodl-guard-lambda-concurrency` alarm went to ALARM at 09:18 that morning and the
  hourly fuse would have held both functions by ~11:07 (~$40); the $40 daily budget e-mails within the day.

## Layer 1 - deploy gate

`python3 scripts/event_graph.py` builds the trigger graph statically:

* **event** edges: an engine publishes event E (`system_events.publish/publish_many`, `Bus.publish`,
  `put_events` DetailType, one-level wrappers, `EVT_*` constants) and `ROUTES[E]["invoke"]` names a target;
* **rule** edges: a rule on bus `justhodl-system-events` listed in `config/eventbridge-bus-rules.json` sends E
  straight to a Lambda (today only the coordinator's catch-all exists);
* **invoke / self** edges: literal `lambda.invoke(FunctionName=...)` targets and `context.function_name`.

A cycle containing an event or rule edge fails every Lambda deploy - it cannot be allowlisted. A cycle of
direct invokes only must be listed with its stop condition in `config/loop-allowlist.json` (7 today: six
self-continuations with depth/hop/shard caps and the a2a bus <-> backend-agent pair, whose bus wakes the agent
only for turns not sent by claude*). Over-approximation is deliberate: a name that may be published counts.

Adding a route, a bus rule or a re-published event: run the script first; give a new busy target its own
budget in `BREAKER_PER_HOUR`; add a new bus rule to `config/eventbridge-bus-rules.json` in the same commit.

## Layer 2 - circuit breaker (coordinator)

Every routed invoke takes one unit from `<target>#<YYYYMMDDHH>` and `global#<YYYYMMDDHH>` in DynamoDB
`justhodl-event-breaker` (atomic conditional ADD, TTL 3 days). Budgets: default 30/h, alpha-compass 60,
engine-signal-map 90, miss-calibrator 6, the whole coordinator 300/h - each at least 2x the target's busiest
normal hour Sep 1 - Oct 11 (ops 6534). Past the budget: the invoke is skipped, the event acknowledged (no
retry), audited, and `BREAKER_OPEN` logged once per target-hour -> metric filter -> alarm
`justhodl-event-breaker-open` -> governor fast fuse + e-mail. If the table cannot answer, the coordinator
refuses every invoke (fail-closed) and alarm `justhodl-event-breaker-unavailable` e-mails.

Raising a budget is a code change (`BREAKER_PER_HOUR` in the coordinator) and goes through the deploy gate.

## Layer 3 - blast-radius caps

Reserved concurrency (ops 6541): coordinator 10, alpha-compass 3, calibrator 2, alpha-calibrator 2,
signal-board 2, miss-calibrator 1, engine-signal-map 2, jh-fusion 2, a2a-bus 5, backend-agent 2. Async event
config on the same functions: 1 retry, 15-minute maximum event age (the storm kept 6-hour backlogs of 24M
events alive). The coordinator's bus-rule target: 2 retries, 15 minutes (EventBridge's default is 24 h / 185).
During the October freeze the governor holds them at 0; its ledger restores these caps on Nov 1.

## Layer 4 - fast fuse

| Alarm | Signal (account level, AWS/Lambda) | Threshold, 3 x 5 min | Normal max held 15 min | Oct 12 storm |
|---|---|---|---|---|
| `justhodl-runaway-concurrency` | ConcurrentExecutions, Average | > 100 | 17.7 | 760 |
| `justhodl-runaway-invocations` | Invocations, Sum | > 8,000 | 5,303 (Oct 5 midnight fan-out) | 35,302 |
| `justhodl-runaway-duration` | Duration, Sum (ms) | > 20,000,000 (~$48/day at 512 MB) | 4.8M | 227M |

Normal = Sep 15 - Oct 11, 7,776 five-minute points. Alarm -> SNS `justhodl-spend-governor` -> governor
`alarm_trigger` -> `fast_fuse`: per-function cost over the 15-minute window, then each 5-minute bucket.
A function is throttled (reserved concurrency 0, ledgered as FUSE) when it burns >= $20/day over the window,
carries >= 10% of the fleet's burn and stayed at that rate in every bucket - top 5 per alarm, at most one run
per 5 minutes. Short legitimate bursts (2026-10-10 16:30, 654 copies for ~4 min) never qualify; the older
guard alarms that do fire on such bursts only speed up the hourly fuse.

Release after the cause is fixed:
`{"action":"restore","include_fuse":true,"only_ids":["<function>"]}` on `justhodl-spend-governor`.
Dry run any time: `{"action":"fast_fuse"}` (same path as an alarm).

## Layer 5 - money backstops

* Spend governor (hourly): two-strike runaway fuse, BRAKE/FREEZE against the $500 monthly budget
  (`docs/ops/SPEND_GOVERNOR.md`).
* AWS Budgets `JustHodl-Daily-Spend-Guard`: $40/day actual -> e-mail (same address as the monthly budget, no
  confirmation needed) + the governor. Normal days cost $15-24.
* Cost Anomaly Detection `Default-Services-Subscription`: daily e-mail at >= $25 and >= 20% impact (was $100 and 40%).
* `justhodl-s3-live-bucket-size`: Standard storage of `justhodl-dashboard-live` > 2 TB (normal 0.6-1.0 TB).
* Lifecycle `ops6541-bound-noncurrent-versions` on `justhodl-dashboard-live`: every object keeps its 10 newest
  previous versions; older noncurrent versions expire after 1 day. A rewrite storm on one key can no longer
  accumulate more than a day of copies (`data/` and `system-events/` already expire all noncurrent after 1 day).

## Alerts reach Khalid by

* e-mail from AWS Budgets and Cost Anomaly Detection (confirmed, direct);
* e-mail from SNS `justhodl-fleet-alerts` - every alarm above and every governor action - **only after Khalid
  clicks "Confirm subscription"** in the AWS e-mail (KHALID_ACTIONS.md item 0). Telegram (`justhodl-guardrail-notify`)
  answers 401 Unauthorized: its bot token is dead until Khalid issues a new one.

## Operating

```bash
python3 scripts/event_graph.py            # the loop map; exit 1 = a loop or an unlisted chain
python3 scripts/event_graph.py --json     # full graph
```

## Proof (2026-10-17, reports in `aws/ops/reports/latest/`)

* Gate: the pre-fix calibrator source fails it; the fleet passes (926 functions, 20 publishers, 33 routes, 37
  edges, 7 documented invoke chains, 0 bus loops); deploy-lambdas 3e977c6f ran it in the preflight and passed.
* Breaker: on real DynamoDB the coordinator's conditional ADD answered ok, ok, open at limit 2 (ops 6541);
  `tests/test_event_coordinator_breaker.py` (8 tests) in the coordinator's deploy gate.
* Caps: all 10 set; the freeze ledger now restores coordinator 10, a2a-bus 5, alpha-compass 3, the others
  2 / miss-calibrator 1 on Nov 1 (ops 6541).
* Alarm -> governor, for real: `justhodl-s3-live-bucket-size` went to ALARM at 01:55:02 on creation (the bucket
  still held the loop's copies), CloudWatch published to the governor topic the same second and the governor
  wrote `spend-governor/events/20261017T015502Z-fast-fuse-check.json` (frozen fleet: nothing hot). A synthetic
  alarm through SNS produced `...020050Z-fast-fuse-check.json` (ops 6542). Fast-fuse throttling itself is
  proven against moto CloudWatch data (700 copies of a 512 MB function = $505/day -> throttled, a quiet
  function untouched) and by 9 governor unit checks.
* Self-check: the governor's guardrail check reports no problems (all five alarms present, enabled, wired).
* Calibration: ops 6534 (Sep 15 - Oct 11 vs Oct 12-16) and ops 6540 (the old alarms' history).
