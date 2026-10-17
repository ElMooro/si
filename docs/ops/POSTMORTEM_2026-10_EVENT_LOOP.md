# Postmortem - the miss-calibrator / event-coordinator loop (2026-10-12 .. 10-16)

Cost: Oct 12-16 billed **$2,369** against a normal **~$21/day**; ~$2,300 of it is the loop (Cost Explorer,
estimated, read 2026-10-17 00:38 UTC by ops 6530). Oct 16-17 S3 storage of the leftover copies was still posting.
Evidence: `aws/ops/reports/latest/ops_6530_loop_forensics.md`, `ops_6531_storm_run_timelines.md`, 6519/6520.

## What happened

1. **The loop was wired on 2026-05-31** (050ceb8c "event-bus coordination layer + first 3 wirings"):
   `justhodl-miss-calibrator` publishes one `near_miss.extreme` event for every signal type with >= 50
   near-misses in 30 days, and `justhodl-event-coordinator` answered `near_miss.extreme` by invoking
   `justhodl-miss-calibrator` again. Three signal types qualified (screener_top_pick 185,
   momentum_top_pick 141, valuation_composite 128), so every calibrator run started three more.
2. **A duplicate filter contained it by accident for 4.5 months.** The same commit's coordinator dropped any
   event whose content matched one seen in the last 60 s in that warm container. The loop's events always had
   identical content, so each Monday's 09:15 UTC run sputtered and died within seconds:
   Sep 14 - 10 calibrator runs, Sep 21 - 16, Sep 28 - 24, Oct 5 - 12 (Oct 5: 12 runs x 3 events = 36
   near_miss.extreme events, 11 routed, 25 dropped as duplicates; + 2 other events = the 38 coordinator runs).
3. **Oct 7 22:04 UTC, 42bedb1c "Retry coordinator targets that were not accepted"** (deployed 22:05) made
   delivery at-least-once: the dedupe key now includes the event's id (`_event_id`, else the EventBridge
   event id - unique per event) and a failed downstream invoke raises so Lambda retries. Sound for losing no
   events; but every lap of the loop now looked new, so nothing was ever dropped. The change was tested for
   delivery semantics; no test exercised the calibrator -> coordinator -> calibrator cycle.
4. **Mon Oct 12 09:15 UTC, the scheduled run lit it.** Branching x3 per lap (1 -> 3 -> 9 -> ... -> 729) reached
   625 concurrent calibrators inside the first minute. From then on the only limit was the account's shared
   Lambda pool: UnreservedConcurrentExecutions sat at its 764 maximum every day until Oct 16.
5. **It ran 4 days 10.5 hours** - stopped Oct 16 19:44:48 UTC by ops 6520 (reserved concurrency 0, async
   retries 0, event age 60 s), then fixed in code (a0629310) and released (6521).

## Why it cost what it cost

* **Lambda $1,951** (116.7 M GB-s + 32.4 M requests): 12.1 M calibrator runs (512 MB) + 21.0 M coordinator runs
  (256 MB). The pool was pinned: ~752 busy slots on average x 86,400 s x ~0.41 GB = ~$450/day.
  A calibrator run normally takes 0.75 s; in the storm 15.3 s, almost all of it in botocore's backoff
  retries of two `ssm:PutParameter` calls that ~700 copies were making to the same two parameters
  (ThrottlingException / TooManyUpdates). Demand was 3x what the pool could run: 24 M queued invocations
  expired after the 6 h maximum event age and went to the shared DLQ.
* **S3 ~$318** so far: the coordinator appended each routed event to `system-events/audit/<day>.jsonl`
  by GET + PUT of the whole day-log (22-33 MB) in the versioned `justhodl-dashboard-live` bucket. Each PUT
  kept the previous copy as a noncurrent version: bucket 0.92 TB (Oct 11) -> 24.7 -> 61.2 -> 105.9 -> 145.4 TB
  (Oct 15). 27.1 M PUTs ($136), storage ($134 posted so far), 60.5 M GETs ($24). Concurrent appends mostly
  overwrote each other, so the day-log stayed ~30 MB while every write still stored a full copy.
* **EventBridge $36** (35.5 M custom events), **CloudWatch Logs $11** (27.4 GB), **SQS $8** (DLQ), the rest small.

## Why nothing stopped it for 4.4 days

* AWS Lambda's recursive-loop detection does not follow a hop through an EventBridge bus.
* Every alert path was Telegram, and the bot token was dead (each run logged `HTTP Error 401`).
* The AWS Budget had no working action and billing data lags about a day; there was no spend governor.
* The trigger was weekly, so the Oct 7 change sat dormant 4.5 days and the link to it was not obvious.

## What changed

* a0629310: the coordinator never invokes the engine that produced the event (`is_self_loop`), the audit
  day-log stops growing at 5 MB, the calibrator no longer emits `near_miss.extreme` and debounces
  event-triggered runs for 6 h. Tests: `tests/test_event_coordinator_loop_guard.py`, calibrator suite.
* ops 6520: lifecycle rule `ops6520-purge-dead-versions-system-events` expires noncurrent `system-events/`
  versions after 1 day (S3 stops billing an expired version at eligibility); ops 6529 deletes the newest days.
* `justhodl-spend-governor` (ops 6522-6524, docs/ops/SPEND_GOVERNOR.md): hourly cap enforcement, and a runaway
  fuse that throttles any single function burning >= $20/day at two checks 0.8-3 h apart - this incident would
  have been throttled at the second hourly tick (~11:07 UTC Oct 12, ~$40).
* Alerts: AWS Budgets emails Khalid directly at 50/80/90/100%; runaway alarms and governor stage changes go
  to SNS `justhodl-fleet-alerts`, which has an email subscription waiting for Khalid's confirmation click
  (KHALID_ACTIONS.md item 0) - until then they still reach only the dead Telegram bot.
* AUTONOMY.md "Known traps" carries the two lessons: never route an event back to its producer, and an
  append by read-modify-write to one object in a versioned bucket stores the whole object on every append.
* 2026-10-17, the "never again" set (`docs/ops/RUNAWAY_GUARDRAILS.md`, ops 6534/6540/6541/6542): a deploy gate
  that refuses any loop through the event bus, a DynamoDB circuit breaker in the coordinator, reserved
  concurrency caps and 15-minute event ages on the bus path, sustained-runaway alarms wired to an automatic
  fast fuse in the spend governor, a $40/day budget and $25 anomaly e-mails, and a bucket-wide bound on
  noncurrent versions.

## The detail that mattered most

The existing alarm `justhodl-guard-lambda-concurrency` went to ALARM at 09:18 on Oct 12 - three minutes into
the loop - and `justhodl-guard-lambda-invocations-1h` at 09:19. Both stayed in ALARM for 4.4 days. Their only
destinations were SNS `justhodl-fleet-alerts` -> `justhodl-guardrail-notify` -> a Telegram bot whose token
answered 401. Detection worked; nothing was listening and nothing acted. The new alarms act on their own.
