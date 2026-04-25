# Force-rerun screener with retry+backoff + 2 workers

**Status:** failure  
**Duration:** 307.1s  
**Finished:** 2026-04-25T23:16:37+00:00  

## Error

```
SystemExit: 1
```

## Log
## A. Verify Lambda deployment

- `23:11:30`   CodeSha256: zvcZXbiIHfpIcAvR...
- `23:11:30`   LastModified: 2026-04-25T23:11:28
- `23:11:30`   Current timeout: 600s, memory: 1024MB
## B. Bump timeout to 900s if needed

- `23:11:30`   Bumping timeout 600s → 900s
- `23:11:32` ✅   Timeout updated to 900s
## C. Force-invoke screener (5-9 min expected)

- `23:16:37` ✗   Invoke failed after 305.5s: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-stock-screener/invocations"
