# ops 5221 -- audit 2026-09-08 Release A2 gate: fleet credential sweep

**Status:** failure  
**Duration:** 159.5s  
**Finished:** 2026-09-09T01:56:54+00:00  

## Error

```
SystemExit: 1
```

## Data

| engines | log_failures | missing_live | not_redeployed | residual_files | step |
|---|---|---|---|---|---|
| 329 | 2 | 0 | 0 | 0 | a2 |

## Log
- `01:54:15` 329 engines read credentials through managed_secret
## 1. redeploy state

- `01:55:03` ✅ every live rewritten engine redeployed after the push
## 2. log scan for managed_secret failures since the push

- `01:56:54` scanned 329 log groups
## 3. residual literals

- `01:56:54` ✅ zero residual credential literals in the tree (excluding aws/lambdas/_archived)
## verdict

- `01:56:54` ✗ fedliquidityapi: [managed_secret] /justhodl/fred/api-key unavailable: An error occurred (AccessDeniedException) when calling the GetParameter operatio

- `01:56:54` ✗ fmp-stock-picks-agent: [managed_secret] /justhodl/fred/api-key unavailable: An error occurred (AccessDeniedException) when calling the GetParameter operatio

- `01:56:54` RED: 2 failure(s)
