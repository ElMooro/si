## 0. Env bundle + live-feed probes from the runner

**Status:** failure  
**Duration:** 16.3s  
**Finished:** 2026-07-07T17:53:48+00:00  

## Error

```
SystemExit: 1
```

## Data

| coingecko_btc_usd | compass_body | compass_fn_error | compass_seconds | compass_status | context | env_keys | fred_DGS1 | fred_DGS2 | fred_EXPINF1YR | polygon_gld_bars | role | schedule |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET'] |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 3.96 | 4.14 | 3.01917236 |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 8 |  |  |
| 64089 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | arn:aws:iam::857687956942:role/justhodl-scheduler-role | created |
|  | {"errorMessage": "list index out of range", "errorType": "IndexError", "requestId": "02926b45-9201-41ad-abee-4a6f7f64f99f", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 481, in lambda_handler\n    px = closes.get(tkr, [None])[-1] if  | Unhandled | 11.6 | 200 |  |  |  |  |  |  |  |  |
|  |  |  |  |  | {} |  |  |  |  |  |  |  |

## Log
## 1. Deploy justhodl-asset-compass

- `17:53:33`   zip: 10313 bytes
## 1. Lambda

- `17:53:33`   Lambda missing — creating
- `17:53:36` ✅   ✓ created justhodl-asset-compass
## 2. EventBridge Scheduler schedule

## 3. Synchronous first run + hard verify

## 4. Sibling context (warn-only)

## verdict

- `17:53:48` FAILS=2 WARNS=1
- `17:53:48` report written: /home/runner/work/si/si/aws/ops/reports/2966.json
