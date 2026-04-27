# Create EB rule for justhodl-khalid-metrics

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-04-27T17:17:42+00:00  

## Log
- `17:17:41`   Lambda:   justhodl-khalid-metrics
- `17:17:41`   Rule:     justhodl-khalid-metrics-refresh
- `17:17:41`   Schedule: cron(0 11 * * ? *)
## 1. Resolve Lambda ARN

- `17:17:42` ✅   ARN: arn:aws:lambda:us-east-1:857687956942:function:justhodl-khalid-metrics
## 2. Create/update EB rule

- `17:17:42` ✅   put_rule OK: arn:aws:events:us-east-1:857687956942:rule/justhodl-khalid-metrics-refresh
## 3. Attach Lambda as target

- `17:17:42` ✅   put_targets OK
## 4. Grant EventBridge invoke permission on Lambda

- `17:17:42` ✅   add_permission OK (StatementId=EventBridgeKhalidMetricsInvoke)
## 5. Verify final state

- `17:17:42`   rule.State:    ENABLED
- `17:17:42`   rule.Schedule: cron(0 11 * * ? *)
- `17:17:42`   target: 1 → arn:aws:lambda:us-east-1:857687956942:function:justhodl-khalid-metrics
- `17:17:42` ✅   ✓ justhodl-khalid-metrics attached as target
- `17:17:42`   target: khalid-metrics → arn:aws:lambda:us-east-1:857687956942:function:justhodl-ka-metrics
- `17:17:42`   Lambda's rules: ['justhodl-khalid-metrics-refresh']
- `17:17:42` ✅   ✓ Lambda <-> rule binding confirmed bidirectionally
## Result

- `17:17:42` ✅ 
  ✅ justhodl-khalid-metrics will now fire daily at 11:00 UTC
- `17:17:42`   Next expected invocation: today 11:00 UTC if it's before then,
- `17:17:42`   otherwise tomorrow 11:00 UTC.
