# ops 4681 — arm + publish the browser grabber

**Status:** failure  
**Duration:** 0.4s  
**Finished:** 2026-08-14T23:33:17+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4681_arm_grabber.py", line 61, in main
    url = ssm.get_parameter(
          ^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ParameterNotFound: An error occurred (ParameterNotFound) when calling the GetParameter operation: 

```

## Log
## 1. Settle the ingest deploy

- `23:33:16` ✅   ingest live
## 2. Verify kind='series' with a synthetic post

