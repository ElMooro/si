# ops 5086 -- justhodl-fortress v2.0.0 deploy + daily + weekly backtest, verified

**Status:** failure  
**Duration:** 3.3s  
**Finished:** 2026-09-01T03:22:48+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5086_fortress_v2.py", line 124, in main
    deploy_lambda(
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 261, in deploy_lambda
    create_or_update_lambda(
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 106, in create_or_update_lambda
    _retry_on_conflict(_lam.update_function_configuration,
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 85, in _retry_on_conflict
    return call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the UpdateFunctionConfiguration operation: 1 validation error detected: Value 'FORTRESS COIL v2: dump-resilient accumulation radar -- 3y SPY-dump capture (two reads), worst-day t-stats, EMA250, Bollinger/Keltner squeeze, volume-structure accumulation, VCP/RS-line structure, tail risk, flows, backlog, floor; weekly walk-forward backtest' at 'description' failed to satisfy constraint: Member must have length less than or equal to 256

```

## Data

| donor | g1 | keys |
|---|---|---|
| justhodl-equity-research | PASS | ['FMP_KEY', 'FORTRESS_VERSION', 'POLYGON_API_KEY'] |

## Log
## G1 key inheritance

## G2 deploy

- `03:22:45`   zip: 143931 bytes
## 1. Lambda

- `03:22:46`   Lambda exists — updating
