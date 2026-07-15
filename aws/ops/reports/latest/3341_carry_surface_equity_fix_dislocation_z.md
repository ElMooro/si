## Deploy carry-surface v1.1.0

**Status:** failure  
**Duration:** 9.3s  
**Finished:** 2026-07-15T15:47:36+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/3341_carry_surface_equity_fix_dislocation_z.py", line 58, in <module>
    smoke = deploy_lambda(
            ^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 261, in deploy_lambda
    create_or_update_lambda(
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 106, in create_or_update_lambda
    _retry_on_conflict(_lam.update_function_configuration,
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 85, in _retry_on_conflict
    return call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the UpdateFunctionConfiguration operation: 1 validation error detected: Value 'UNIVERSAL CARRY SURFACE — institutional cross-asset carry engine. Answers: 'which asset is the market paying me most to hold, right now?' across equity / FX / FI / commodity / crypto. Z-scored within class, ranked cross-asset, with carry-momentum vs 7D/30D snapshots and risk-adjusted (Sharpe-of-carry) leaders.' at 'description' failed to satisfy constraint: Member must have length less than or equal to 256

```

## Log
- `15:47:27`   zip: 86302 bytes
## 1. Lambda

- `15:47:27`   Lambda exists — updating
