## 1. Settle + idempotent env + EB rule

**Status:** failure  
**Duration:** 71.9s  
**Finished:** 2026-07-08T00:54:34+00:00  

## Error

```
SystemExit: 1
```

## Data

| body | env_keys | env_vars | err | secs | update |
|---|---|---|---|---|---|
|  | ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET'] |  |  |  |  |
|  |  | 4 |  |  | Successful |
| {"errorMessage": "SPY history short: 0", "errorType": "RuntimeError", "requestId": "630dea78-a58c-4a23-8a7a-c6f9c4d3e79a", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 219, in lambda_ |  |  | Unhandled | 4.3 |  |

## Log
- `00:54:30` ✅   ✓ created rule industry-rotation-daily
- `00:54:30` ✅   ✓ target → justhodl-industry-rotation
- `00:54:30` ✅   ✓ added invoke permission
## 2. Invoke

- `00:54:34` FAILS=1 WARNS=0
