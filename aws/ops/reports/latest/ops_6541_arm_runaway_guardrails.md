# ops 6541 - arm the runaway guardrails and prove them

**Status:** failure  
**Duration:** 36.4s  
**Finished:** 2026-10-17T01:56:07+00:00  

## Error

```
SystemExit: 1
```

## Log
## 0. Code deployed

- `01:55:31` ✅ governor reports v1.1.0 (stage FREEZE, held {'FREEZE/lambda': 914, 'FREEZE/smasync': 1})
- `01:55:31` coordinator LastModified 2026-10-17T01:53:13.000+0000 CodeSha256 7lrXkXdQkZYR...
## A. Breaker table

- `01:55:41` ✅ table justhodl-event-breaker created
- `01:55:41` ✅ TTL on expires_at enabled
- `01:55:41` ✅ breaker conditional ADD on real DynamoDB: ['ok', 'ok', 'open'] (limit 2)
## B. Caps + async config (carried through the freeze)

- `01:55:44` ✅ caps + async config set: {"justhodl-event-coordinator": 10, "justhodl-alpha-compass": 3, "justhodl-calibrator": 2, "justhodl-alpha-calibrator": 2, "justhodl-signal-board": 2, "justhodl-miss-calibrator": 1, "justhodl-engine-signal-map": 2, "justhodl-jh-fusion": 2, "justhodl-a2a-bus": 5, "justhodl-backend-agent": 2}
- `01:56:03` governor tick: stage=FREEZE drift={"new": {}, "reapplied": {"applied": 10, "failed": 1}}
- `01:56:04` after tick: {"justhodl-event-coordinator": {"now": 0, "restore_to": 10, "stage": "FREEZE"}, "justhodl-alpha-compass": {"now": 0, "restore_to": 3, "stage": "FREEZE"}, "justhodl-calibrator": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-alpha-calibrator": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-signal-board": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-miss-calibrator": {"now": 0, "restore_to": 1, "stage": "FREEZE"}, "justhodl-engine-signal-map": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-jh-fusion": {"now": 0, "restore_to": 2, "stage": "FREEZE"}, "justhodl-a2a-bus": {"now": 0, "restore_to": 5, "stage": "FREEZE"}, "justhodl-backend-agent": {"now": 0, "restore_to": 2, "stage": "FREEZE"}}
- `01:56:04` ✅ frozen at 0 now; the Nov 1 restore puts back the caps
## C. Bus delivery retry policy

- `01:56:04` ✅ bus rule targets now: [('justhodl-event-coordinator', {'MaximumRetryAttempts': 2, 'MaximumEventAgeInSeconds': 900})]
## D. Governor topic accepts alarms

- `01:56:04` ✅ governor topic: ['Owner', 'BudgetsPublish', 'CloudWatchAlarmsPublish']
## E. Breaker metric filters

- `01:56:04` ✅ metric filters on /aws/lambda/justhodl-event-coordinator: ['justhodl-event-breaker-open', 'justhodl-event-breaker-unavailable']
## F. Alarms

- `01:56:05`   justhodl-runaway-concurrency ('OK', 100.0, ['justhodl-fleet-alerts', 'justhodl-spend-governor'])
- `01:56:05`   justhodl-runaway-invocations ('OK', 8000.0, ['justhodl-spend-governor', 'justhodl-fleet-alerts'])
- `01:56:05`   justhodl-runaway-duration ('INSUFFICIENT_DATA', 20000000.0, ['justhodl-fleet-alerts', 'justhodl-spend-governor'])
- `01:56:05`   justhodl-event-breaker-open ('INSUFFICIENT_DATA', 1.0, ['justhodl-fleet-alerts', 'justhodl-spend-governor'])
- `01:56:05`   justhodl-event-breaker-unavailable ('OK', 1.0, ['justhodl-fleet-alerts'])
- `01:56:05`   justhodl-s3-live-bucket-size ('ALARM', 2000000000000.0, ['justhodl-spend-governor', 'justhodl-fleet-alerts'])
- `01:56:05` ✅ 6 alarms armed
## G. Budget + anomaly e-mail backstops

- `01:56:06` ✅ budget JustHodl-Daily-Spend-Guard exists; limit kept at $40/day
- `01:56:07` ✅ anomaly alerts now: {"And": [{"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_ABSOLUTE", "Values": ["25.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}}, {"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_PERCENTAGE", "Values": ["20.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}}]}
## H. Versioned-bucket bound

- `01:56:07` ✅ lifecycle ops6541-bound-noncurrent-versions present (11 rules total): {'NoncurrentDays': 1, 'NewerNoncurrentVersions': 10}
## I. End to end: forced alarm -> governor

- `01:56:07` ✗ end_to_end failed: ClientError: An error occurred (AccessDenied) when calling the SetAlarmState operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: cloudwatch:SetAlarmState on resource: arn:aws:cloudwatch:us-east-1:857687956942:alarm:justhodl-runaway-invocations because no identity
## Note: owned SageMaker endpoint lever

- `01:56:07` smasync ledger entry: {"status": "failed", "error": "ValidationException", "error_msg": "User is missing the following permissions: cloudwatch:PutMetricAlarm, cloudwatch:DeleteAlarms", "failed_at": "2026-10-17T01:55:44.912478+00:00", "scale_down_note": null, "applied_at": null}
## Reversal

- `01:56:07` caps: put_function_concurrency back / governor ledger; alarms: delete_alarms justhodl-runaway-*, justhodl-event-breaker-*, justhodl-s3-live-bucket-size; budget: delete_budget JustHodl-Daily-Spend-Guard; lifecycle: drop rule ops6541-bound-noncurrent-versions; topic: drop Sid CloudWatchAlarmsPublish; table: delete justhodl-event-breaker
## Errors

- `01:56:07` ✗ end_to_end (critical): ClientError: An error occurred (AccessDenied) when calling the SetAlarmState operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: cloudwatch:SetAlarmState on resource: arn:aws:cloudwatch:us-east-1:857687956942:alarm:justhodl-runaway-invocations because no identity
