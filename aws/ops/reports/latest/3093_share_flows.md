## 1. Function + schedule

**Status:** failure  
**Duration:** 0.7s  
**Finished:** 2026-07-11T03:08:10+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3093_share_flows.py", line 115, in main
    if not ensure_function(rep, fails):
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/ops_3093_share_flows.py", line 58, in ensure_function
    L.update_function_code(FunctionName=FN,
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceConflictException: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The operation cannot be performed at this time. The resource arn:aws:lambda:us-east-1:857687956942:function:justhodl-share-flows is currently in the following state: 'Pending'. StateReasonCode: 'Creating'

```

## Log

