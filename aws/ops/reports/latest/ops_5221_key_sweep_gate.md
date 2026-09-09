# ops 5221 -- audit 2026-09-08 Release A2 gate: fleet credential sweep

**Status:** failure  
**Duration:** 1965.6s  
**Finished:** 2026-09-09T01:17:39+00:00  

## Error

```
SystemExit: 1
```

## Data

| engines | log_failures | missing_live | not_redeployed | residual_files | step |
|---|---|---|---|---|---|
| 329 | 2 | 0 | 4 | 1 | a2 |

## Log
- `00:44:53` 329 engines read credentials through managed_secret
## 1. redeploy state

- `00:45:38`    waiting: 4 not yet redeployed (44s)
- `00:47:39`    waiting: 4 not yet redeployed (165s)
- `00:49:39`    waiting: 4 not yet redeployed (285s)
- `00:51:40`    waiting: 4 not yet redeployed (406s)
- `00:53:40`    waiting: 4 not yet redeployed (527s)
- `00:55:41`    waiting: 4 not yet redeployed (647s)
- `00:57:41`    waiting: 4 not yet redeployed (768s)
- `00:59:42`    waiting: 4 not yet redeployed (888s)
- `01:01:43`    waiting: 4 not yet redeployed (1009s)
- `01:03:43`    waiting: 4 not yet redeployed (1129s)
- `01:05:44`    waiting: 4 not yet redeployed (1250s)
- `01:07:44`    waiting: 4 not yet redeployed (1370s)
- `01:09:45`    waiting: 4 not yet redeployed (1491s)
- `01:11:45`    waiting: 4 not yet redeployed (1612s)
- `01:13:46`    waiting: 4 not yet redeployed (1732s)
## 2. log scan for managed_secret failures since the push

- `01:17:38` scanned 329 log groups
## 3. residual literals

## verdict

- `01:17:39` ✗ 4 engines not redeployed after 1852s: justhodl-activity-nowcast, justhodl-ai-chat, justhodl-credit-stress, justhodl-volatility-squeeze-hunter
- `01:17:39` ✗ fedliquidityapi: [managed_secret] /justhodl/fred/api-key unavailable: An error occurred (AccessDeniedException) when calling the GetParameter operatio

- `01:17:39` ✗ fmp-stock-picks-agent: [managed_secret] /justhodl/fred/api-key unavailable: An error occurred (AccessDeniedException) when calling the GetParameter operatio

- `01:17:39` ✗ 1 files still carry a live credential literal: ['cloudflare/workers/justhodl-data-proxy/src/index.js']
- `01:17:39` RED: 4 failure(s)
