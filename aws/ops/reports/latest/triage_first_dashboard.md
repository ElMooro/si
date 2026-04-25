# Triage first dashboard run + add IAM perms

**Status:** success  
**Duration:** 14.4s  
**Finished:** 2026-04-25T01:00:40+00:00  

## Data

| iam_policy_added | known_broken_logic | next_step |
|---|---|---|
| HealthMonitorEventBridgeRead | now always 'info' regardless | step 84 builds HTML dashboard |

## Log
## 1+2. Check actual ages of red/yellow components

- `01:00:26`   edge-data.json                 age=2.9h size=1,222B
- `01:00:26`   repo-data.json                 age=1.5h size=36,413B
- `01:00:26`   screener/data.json             age=5.5h size=326,603B
## 3. Add EventBridge read perms to lambda-execution-role

- `01:00:26` ✅   Attached inline policy HealthMonitorEventBridgeRead
## 4. Update lambda_function.py — known_broken should show as 'info'

- `01:00:26` ✅   Updated known_broken handling (forces 'info' regardless of status)
## Re-deploy with fixes + re-invoke

- `01:00:30` ✅   Re-deployed: 6507 bytes
- `01:00:40` ✅   Re-invoke status: 200
- `01:00:40` 
  System: red
- `01:00:40`   Counts: {'green': 24, 'yellow': 2, 'red': 1, 'info': 2, 'unknown': 0}
- `01:00:40` 
  Non-green components after fixes:
- `01:00:40`     [red    ] critical     s3:edge-data.json                                         age=2.9h  
- `01:00:40`     [yellow ] critical     s3:repo-data.json                                         age=1.5h  
- `01:00:40`     [yellow ] important    s3:screener/data.json                                     age=5.5h  
- `01:00:40` Done
