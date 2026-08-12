# ops 4638 — integrity + grammar + NQ door

**Status:** failure  
**Duration:** 4.2s  
**Finished:** 2026-08-12T15:52:51+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4638_integrity_and_nq_gate.py", line 166, in main
    lam.update_function_configuration(
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceConflictException: An error occurred (ResourceConflictException) when calling the UpdateFunctionConfiguration operation: The operation cannot be performed at this time. An update is in progress for resource: arn:aws:lambda:us-east-1:857687956942:function:justhodl-liquidity-reversal

```

## Data

| bytes | proxy | smoke |
|---|---|---|
| 142 | https://justhodl-nq-proxy.raafouis.workers.dev | PASS |

## Log
## NQ egress worker (Cloudflare)

