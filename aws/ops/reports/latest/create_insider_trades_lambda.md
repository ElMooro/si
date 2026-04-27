# Create justhodl-insider-trades Lambda + EB rule

**Status:** failure  
**Duration:** 0.5s  
**Finished:** 2026-04-27T18:05:10+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/_create_insider_trades_lambda.py", line 243, in main
    update_function(zip_bytes, r)
  File "/home/runner/work/si/si/aws/ops/pending/_create_insider_trades_lambda.py", line 138, in update_function
    lam.update_function_code(FunctionName=FN_NAME, ZipFile=zip_bytes)
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceConflictException: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The operation cannot be performed at this time. An update is in progress for resource: arn:aws:lambda:us-east-1:857687956942:function:justhodl-insider-trades

```

## Log
- `18:05:10`   zip: 7513 bytes
## 1. Lambda function

- `18:05:10`   Lambda exists — updating code + config
