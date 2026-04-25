# Force daily-report-v3 invoke + verify IBIT history

**Status:** failure  
**Duration:** 64.6s  
**Finished:** 2026-04-25T18:51:24+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/153_force_ibit_population.py", line 31, in <module>
    resp = lam.invoke(
           ^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.TooManyRequestsException: An error occurred (TooManyRequestsException) when calling the Invoke operation (reached max retries: 4): Rate Exceeded.

```

## Log
## 1. Force-invoke daily-report-v3

