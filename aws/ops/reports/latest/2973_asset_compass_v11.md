## 1. Wait for the parallel deploy-lambdas code update

**Status:** failure  
**Duration:** 171.3s  
**Finished:** 2026-07-07T20:10:02+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_min | assets_n | body | code_sha | corr_stamped | deploy_age_s | elapsed_engine_s | env_var_count | er_modeled | excess_stamped | fn_error | invoke_seconds | page_v2_live | priced | reads_with_lines | rf | schema | spy_corr | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | hHnYwWK55EVz |  | 7 |  | 4 |  |  |  |  |  |  |  |  |  |  |  |
|  |  | {"statusCode": 200, "body": "{\"ok\": true, \"assets\": 31, \"er_modeled\": 20, \"warns\": 0}"} |  |  |  |  |  |  |  | None | 51.1 |  |  |  |  |  |  | 200 |
| 0.0 | 31 |  |  |  |  | 49.7 |  |  |  |  |  |  |  |  |  | 1.1 |  |  |
|  |  |  |  |  |  |  |  | 20 |  |  |  |  | 31 |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 20 |  |  |  |  |  | 3.96 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 31 |  |  |  |  |
|  |  |  |  | 30 |  |  |  |  |  |  |  |  |  |  |  |  | 1.0 |  |
|  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |

## Log
## 2. Synchronous run

## 3. Hard verify schema 1.1

## 4. Live page v2

- `20:10:02` ✗ public JSON not yet 1.1 (CDN cache?)
- `20:10:02` FAILS=1 WARNS=0
- `20:10:02` report written: /home/runner/work/si/si/aws/ops/reports/2973.json
