# ops 3963 — self-heal deploy justhodl-domain-barometers

**Status:** failure  
**Duration:** 0.6s  
**Finished:** 2026-07-27T04:56:10+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3965_polarity_fix.py", line 92, in main
    lam.update_function_code(FunctionName=FN, ZipFile=blob, Publish=True)
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceConflictException: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The operation cannot be performed at this time. An update is in progress for resource: arn:aws:lambda:us-east-1:857687956942:function:justhodl-domain-barometers

```

## Data

| donor | marker_in_source | role | runtime | zip_bytes |
|---|---|---|---|---|
|  | True |  |  | 11885 |
| justhodl-tradingview |  | arn:aws:iam::857687956942:role/lambda-execution-role | python3.12 |  |

## Log
## A. diagnose

- `04:56:09`   function EXISTS state=Active modified=2026-07-27T04:56:09.000+0000
## B. create or update from the runner

