# ops 4808 -- justhodl-sp500 birth verify

**Status:** failure  
**Duration:** 6.5s  
**Finished:** 2026-08-17T02:32:57+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4808_sp500_engine_deploy_verify.py", line 176, in main
    heal_fred(rep)
  File "/home/runner/work/si/si/aws/ops/pending/ops_4808_sp500_engine_deploy_verify.py", line 81, in heal_fred
    src = lam.get_function_configuration(
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceNotFoundException: An error occurred (ResourceNotFoundException) when calling the GetFunctionConfiguration operation: Function not found: arn:aws:lambda:us-east-1:857687956942:function:justhodl-dollar-strength-agent

```

## Data

| mem | runtime | state |
|---|---|---|
| 1024 | python3.12 | Active |

## Log
## 1. function Active + env heal

