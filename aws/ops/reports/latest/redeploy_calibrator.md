# 1) Redeploy calibrator with slim-summary SSM fix

**Status:** failure  
**Duration:** 0.2s  
**Finished:** 2026-05-04T21:27:30+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/_redeploy_calibrator.py", line 37, in main
    lam.update_function_code(FunctionName="justhodl-calibrator", ZipFile=zb)
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceConflictException: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The operation cannot be performed at this time. An update is in progress for resource: arn:aws:lambda:us-east-1:857687956942:function:justhodl-calibrator

```

## Log
- `21:27:30`   zip size: 7,314b
