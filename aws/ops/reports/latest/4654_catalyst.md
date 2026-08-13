# ops 4654 — catalyst engine

**Status:** failure  
**Duration:** 1.2s  
**Finished:** 2026-08-13T22:05:16+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4654_catalyst.py", line 119, in main
    inv = lam.invoke(FunctionName=FN,
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceConflictException: An error occurred (ResourceConflictException) when calling the Invoke operation: The operation cannot be performed at this time. The function is currently in the following state: Pending

```

## Log
## deploy (create-capable) + settle + schedule

- `22:05:15` ✅   [deploy] v1.0.0 live (created=True)
- `22:05:16` hourly schedule created
## run + catalyst truth

