# ops 5221 -- audit 2026-09-08 Release A2 gate: fleet credential sweep

**Status:** failure  
**Duration:** 5033.1s  
**Finished:** 2026-09-08T23:28:30+00:00  

## Error

```
SystemExit: 1
```

## Data

| engines | log_failures | missing_live | not_redeployed | residual_files | step |
|---|---|---|---|---|---|
| 329 | 2 | 0 | 137 | 1 | a2 |

## Log
- `22:04:37` 329 engines read credentials through managed_secret
## 1. redeploy state

- `22:05:17`    waiting: 328 not yet redeployed (40s)
- `22:07:56`    waiting: 321 not yet redeployed (199s)
- `22:10:37`    waiting: 313 not yet redeployed (359s)
- `22:13:12`    waiting: 306 not yet redeployed (515s)
- `22:15:50`    waiting: 299 not yet redeployed (673s)
- `22:18:25`    waiting: 292 not yet redeployed (827s)
- `22:21:01`    waiting: 285 not yet redeployed (984s)
- `22:23:36`    waiting: 280 not yet redeployed (1139s)
- `22:26:10`    waiting: 275 not yet redeployed (1293s)
- `22:28:41`    waiting: 270 not yet redeployed (1444s)
- `22:31:14`    waiting: 264 not yet redeployed (1597s)
- `22:33:45`    waiting: 260 not yet redeployed (1748s)
- `22:36:17`    waiting: 255 not yet redeployed (1900s)
- `22:38:48`    waiting: 248 not yet redeployed (2051s)
- `22:41:19`    waiting: 241 not yet redeployed (2202s)
- `22:43:48`    waiting: 237 not yet redeployed (2351s)
- `22:46:17`    waiting: 230 not yet redeployed (2499s)
- `22:48:44`    waiting: 226 not yet redeployed (2647s)
- `22:51:12`    waiting: 219 not yet redeployed (2795s)
- `22:53:37`    waiting: 212 not yet redeployed (2940s)
- `22:56:03`    waiting: 206 not yet redeployed (3086s)
- `22:58:26`    waiting: 202 not yet redeployed (3229s)
- `23:00:51`    waiting: 197 not yet redeployed (3374s)
- `23:03:14`    waiting: 192 not yet redeployed (3517s)
- `23:05:38`    waiting: 188 not yet redeployed (3661s)
- `23:08:00`    waiting: 180 not yet redeployed (3803s)
- `23:10:23`    waiting: 174 not yet redeployed (3946s)
- `23:12:44`    waiting: 167 not yet redeployed (4087s)
- `23:15:05`    waiting: 159 not yet redeployed (4228s)
- `23:17:23`    waiting: 154 not yet redeployed (4366s)
- `23:19:43`    waiting: 150 not yet redeployed (4506s)
- `23:22:00`    waiting: 143 not yet redeployed (4643s)
- `23:24:18`    waiting: 137 not yet redeployed (4781s)
## 2. log scan for managed_secret failures since the push

- `23:28:29` scanned 329 log groups
## 3. residual literals

## verdict

- `23:28:30` ✗ 137 engines not redeployed after 4901s: justhodl-activity-nowcast, justhodl-ai-chat, justhodl-credit-stress, justhodl-nobrainer-tracker, justhodl-opportunity-engine, justhodl-options-analytics, justhodl-options-flow, justhodl-options-flow-scanner, justhodl-outcome-checker, justhodl-pair-trades, justhodl-pairs-arb, justhodl-pairs-scanner, justhodl-paper-book, justhodl-pead-detector, justhodl-plumbing-aggregator, justhodl-pnl-attribution, justhodl-pnl-tracker, justhodl-political-intel, justhodl-political-trades, justhodl-polygon-futures-curves, justhodl-polygon-fx-regime, justhodl-polygon-options-flow, justhodl-portfolio-analytics, justhodl-positioning-analog, justhodl-ppi-acceleration
- `23:28:30` ✗ fedliquidityapi: [managed_secret] /justhodl/fred/api-key unavailable: An error occurred (AccessDeniedException) when calling the GetParameter operatio

- `23:28:30` ✗ fmp-stock-picks-agent: [managed_secret] /justhodl/fred/api-key unavailable: An error occurred (AccessDeniedException) when calling the GetParameter operatio

- `23:28:30` ✗ 1 files still carry a live credential literal: ['cloudflare/workers/justhodl-data-proxy/src/index.js']
- `23:28:30` RED: 4 failure(s)
