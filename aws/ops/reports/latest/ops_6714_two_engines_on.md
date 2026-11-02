# ops 6714 - treasury-api + macro-financial-intelligence: deployed fixes, triggers, one run each

**Status:** failure  
**Duration:** 3.8s  
**Finished:** 2026-11-02T16:35:04+00:00  

## Error

```
SystemExit: 1
```

## Log
## A. deployed code

- `16:35:01` ✅ treasury-api: fix in deployed code True | modified 2026-11-02T16:29:36 | reserved concurrency None
- `16:35:02` ✅ macro-financial-intelligence: fix in deployed code True | modified 2026-11-02T16:29:11 | reserved concurrency None
## B. triggers + the deployed manifest copy

- `16:35:03` macro-financial-daily-execution before: DISABLED rate(1 day) targets ['macro-financial-intelligence']
- `16:35:03` ✅ macro-financial-daily-execution now ENABLED
- `16:35:04` scheduler role justhodl-scheduler-role may invoke treasury-api: implicitDeny
- `16:35:04` ✗ the shared scheduler role cannot invoke treasury-api - not created
