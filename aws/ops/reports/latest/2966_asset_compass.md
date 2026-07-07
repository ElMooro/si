## 0. Env bundle + live-feed probes from the runner

**Status:** failure  
**Duration:** 1.4s  
**Finished:** 2026-07-07T17:48:51+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_2966_asset_compass.py", line 137, in main
    deploy_lambda(report=rep, function_name=FN,
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 248, in deploy_lambda
    create_or_update_lambda(
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 102, in create_or_update_lambda
    _lam.create_function(
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the CreateFunction operation: 1 validation error detected: Value 'Asset Compass - forward-looking cross-asset expected-return + asymmetry engine. Market-implied next-12m rf/inflation (curve + Cleveland Fed), Grinold-Kroner ER per class with published components, upside/downside asymmetry with survival gate, gold/silver breakout scan, data-fit gold-vs-real-rate beta. data/asset-compass.json. PROVISIONAL.' at 'description' failed to satisfy constraint: Member must have length less than or equal to 256

```

## Data

| coingecko_btc_usd | env_keys | fred_DGS1 | fred_DGS2 | fred_EXPINF1YR | polygon_gld_bars |
|---|---|---|---|---|---|
|  | ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET'] |  |  |  |  |
|  |  | 3.96 | 4.14 | 3.01917236 |  |
|  |  |  |  |  | 8 |
| 64108 |  |  |  |  |  |

## Log
## 1. Deploy justhodl-asset-compass

- `17:48:51`   zip: 10313 bytes
## 1. Lambda

- `17:48:51`   Lambda missing — creating
