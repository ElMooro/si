## 1. Ensure engine

**Status:** failure  
**Duration:** 0.7s  
**Finished:** 2026-07-10T04:11:22+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3044_phase_detector.py", line 82, in main
    if not ensure_fn(rep):
           ^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/ops_3044_phase_detector.py", line 67, in ensure_fn
    LAM.create_function(
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceConflictException: An error occurred (ResourceConflictException) when calling the CreateFunction operation: Function already exist: justhodl-phase-detector

```

## Data

| action | fn_exists |
|---|---|
| boto3 create_function | False |

## Log

