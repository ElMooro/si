## 1. Settle + env fix

**Status:** failure  
**Duration:** 77.9s  
**Finished:** 2026-07-08T00:41:00+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_2993_ir_envfix.py", line 102, in main
    ensure_schedule(rep)
  File "/home/runner/work/si/si/aws/ops/pending/ops_2993_ir_envfix.py", line 55, in ensure_schedule
    SCHED.create_schedule(**body)
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1051, in _make_api_call
    request_dict = self._convert_to_request_dict(
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1118, in _convert_to_request_dict
    request_dict = self._serializer.serialize_to_request(
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/validate.py", line 424, in serialize_to_request
    raise ParamValidationError(report=report.generate_report())
botocore.exceptions.ParamValidationError: Parameter validation failed:
Invalid type for parameter Target.RoleArn, value: None, type: <class 'NoneType'>, valid types: <class 'str'>

```

## Data

| env_keys | env_vars | update |
|---|---|---|
| ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET'] |  |  |
|  | 4 | Successful |

## Log

