# ops 6541 - arm the runaway guardrails and prove them

**Status:** failure  
**Duration:** 96.8s  
**Finished:** 2026-10-17T01:54:26+00:00  

## Error

```
SystemExit: 1
```

## Log
## 0. Code deployed

- `01:53:52` ✅ governor reports v1.1.0 (stage FREEZE, held {'FREEZE/lambda': 914, 'FREEZE/smasync': 1})
- `01:53:53` coordinator LastModified 2026-10-17T01:53:13.000+0000 CodeSha256 7lrXkXdQkZYR...
## A. Breaker table

- `01:53:53` ✗ table failed: ClientError: An error occurred (ValidationException) when calling the CreateTable operation: The Tag Value provided is invalid, Value: coordinator circuit breaker (ops 6541)
## B. Caps + async config (carried through the freeze)

- `01:53:56` ✅ caps + async config set: {"justhodl-event-coordinator": 10, "justhodl-alpha-compass": 3, "justhodl-calibrator": 2, "justhodl-alpha-calibrator": 2, "justhodl-signal-board": 2, "justhodl-miss-calibrator": 1, "justhodl-engine-signal-map": 2, "justhodl-jh-fusion": 2, "justhodl-a2a-bus": 5, "justhodl-backend-agent": 2}
- `01:54:20` governor tick: stage=FREEZE drift={"new": {}, "reapplied": {"applied": 10, "failed": 1}}
- `01:54:21` after tick: {"justhodl-event-coordinator": {"now": 0, "restore_to": 10, "stage": "FREEZE"}, "justhodl-alpha-compass": {"now": 0, "restore_to": 3, "stage": "FREEZE"}, "justhodl-calibrator": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-alpha-calibrator": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-signal-board": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-miss-calibrator": {"now": 0, "restore_to": 1, "stage": "FREEZE"}, "justhodl-engine-signal-map": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-jh-fusion": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-a2a-bus": {"now": 0, "restore_to": 5, "stage": "FREEZE"}, "justhodl-backend-agent": {"now": 0, "restore_to": 2, "stage": "FREEZE"}}
- `01:54:21` ✅ frozen at 0 now; the Nov 1 restore puts back the caps
## C. Bus delivery retry policy

- `01:54:21` ✅ bus rule targets now: [('justhodl-event-coordinator', {'MaximumRetryAttempts': 2, 'MaximumEventAgeInSeconds': 900})]
## D. Governor topic accepts alarms

- `01:54:22` ✅ governor topic: ['Owner', 'BudgetsPublish', 'CloudWatchAlarmsPublish']
## E. Breaker metric filters

- `01:54:22` ✅ metric filters on /aws/lambda/justhodl-event-coordinator: ['justhodl-event-breaker-open', 'justhodl-event-breaker-unavailable']
## F. Alarms

- `01:54:23`   justhodl-runaway-concurrency ('INSUFFICIENT_DATA', 100.0, ['justhodl-spend-governor', 'justhodl-fleet-alerts'])
- `01:54:23`   justhodl-runaway-invocations ('INSUFFICIENT_DATA', 8000.0, ['justhodl-spend-governor', 'justhodl-fleet-alerts'])
- `01:54:23`   justhodl-runaway-duration ('INSUFFICIENT_DATA', 20000000.0, ['justhodl-fleet-alerts', 'justhodl-spend-governor'])
- `01:54:23`   justhodl-event-breaker-open ('INSUFFICIENT_DATA', 1.0, ['justhodl-spend-governor', 'justhodl-fleet-alerts'])
- `01:54:23`   justhodl-event-breaker-unavailable ('INSUFFICIENT_DATA', 1.0, ['justhodl-fleet-alerts'])
- `01:54:23`   justhodl-s3-live-bucket-size ('INSUFFICIENT_DATA', 2000000000000.0, ['justhodl-spend-governor', 'justhodl-fleet-alerts'])
- `01:54:23` ✅ 6 alarms armed
## G. Budget + anomaly e-mail backstops

- `01:54:24` ✅ budget JustHodl-Daily-Spend-Guard created: $40/day actual -> 1 e-mail + governor
- `01:54:25` ✅ anomaly alerts now: {"And": [{"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_ABSOLUTE", "Values": ["25.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}}, {"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_PERCENTAGE", "Values": ["20.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}}]}
## H. Versioned-bucket bound

- `01:54:26` ✅ lifecycle ops6541-bound-noncurrent-versions present (11 rules total): {'NoncurrentDays': 1, 'NewerNoncurrentVersions': 10}
## Reversal

- `01:54:26` caps: put_function_concurrency back / governor ledger; alarms: delete_alarms justhodl-runaway-*, justhodl-event-breaker-*, justhodl-s3-live-bucket-size; budget: delete_budget JustHodl-Daily-Spend-Guard; lifecycle: drop rule ops6541-bound-noncurrent-versions; topic: drop Sid CloudWatchAlarmsPublish; table: delete justhodl-event-breaker
## Errors

- `01:54:26` ✗ table (critical): ClientError: An error occurred (ValidationException) when calling the CreateTable operation: The Tag Value provided is invalid, Value: coordinator circuit breaker (ops 6541)
