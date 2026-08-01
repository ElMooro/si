# ops 4237 — declarative schedule reconciliation

**Status:** failure  
**Duration:** 90.9s  
**Finished:** 2026-08-01T14:50:12+00:00  

## Error

```
SystemExit: FAILS: reconciler invoke error
```

## Data

| dupes_dropped | enabled | rules | schedules | section |
|---|---|---|---|---|
| 4 | 690 | 445 | 276 | manifest |

## Log
## 1. Snapshot live AWS into the authoritative manifest

- `14:49:47` captured 445 EventBridge rules + 276 Scheduler schedules (690 enabled)
- `14:49:47` duplicate targets dropped during capture: 4
- `14:49:48` ✅ manifest written to repo config/ and s3://justhodl-dashboard-live/config/schedule-manifest.json
## 2. Deploy the reconciler (AUDIT mode)

- `14:49:48` ✅ SSM /justhodl/schedules/mode = audit
- `14:49:54` ✅ updated
- `14:49:55` ✅ zip marker verified
## 3. PROVE the snapshot — drift must be exactly 0

- `14:50:10` reconciler -> {"errorMessage": "An error occurred (AccessDeniedException) when calling the ListSchedules operation: User: arn:aws:sts::857687956942:assumed-role/lambda-execution-role/justhodl-schedule-reconciler is not authorized to perform: scheduler:ListSchedules on resource: arn:aws:scheduler:us-east-1:8576879
## 4. Schedule the reconciler daily

- `14:50:11` ✅ cron(30 7 * * ? *) -> justhodl-schedule-reconciler (1 target)
- `14:50:12` ✅ manifest updated to include the two new control-plane rules
## RESULT

- `14:50:12` ✗   reconciler invoke error
