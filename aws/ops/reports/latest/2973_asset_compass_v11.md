## 1. Wait for the parallel deploy-lambdas code update

**Status:** success  
**Duration:** 51.2s  
**Finished:** 2026-07-07T20:12:20+00:00  

## Data

| age_min | assets_n | body | code_sha | corr_stamped | deploy_age_s | elapsed_engine_s | env_var_count | er_modeled | excess_stamped | fn_error | invoke_seconds | page_v2_live | priced | public_json_v11 | reads_with_lines | rf | schema | spy_corr | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | hHnYwWK55EVz |  | 249 |  | 4 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | {"statusCode": 200, "body": "{\"ok\": true, \"assets\": 31, \"er_modeled\": 20, \"warns\": 0}"} |  |  |  |  |  |  |  | None | 50.4 |  |  |  |  |  |  |  | 200 |
| 0.0 | 31 |  |  |  |  | 49.4 |  |  |  |  |  |  |  |  |  |  | 1.1 |  |  |
|  |  |  |  |  |  |  |  | 20 |  |  |  |  | 31 |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 20 |  |  |  |  |  |  | 3.96 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 31 |  |  |  |  |
|  |  |  |  | 30 |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.0 |  |
|  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |

## Log
## 2. Synchronous run

## 3. Hard verify schema 1.1

## 4. Live page v2

- `20:12:20` ✅ v1.1 LIVE: 31 assets / 20 modeled; HYG ER 3.39% (carry 5.9, OAS pctile 10.3); reads+horizons on all; corr on 30; UNG barred; page v2 live
- `20:12:20` FAILS=0 WARNS=0
- `20:12:20` report written: /home/runner/work/si/si/aws/ops/reports/2973.json
