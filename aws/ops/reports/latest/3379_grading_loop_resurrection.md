# ops 3379 — signals grading loop resurrection

**Status:** failure  
**Duration:** 316.0s  
**Finished:** 2026-07-17T05:14:49+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3379_grading_loop_resurrection.py", line 226, in <module>
    main(_rep)
  File "/home/runner/work/si/si/aws/ops/pending/ops_3379_grading_loop_resurrection.py", line 160, in main
    resp = LAM.invoke(FunctionName=CHECKER, InvocationType="RequestResponse", Payload=b"{}")
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.TooManyRequestsException: An error occurred (TooManyRequestsException) when calling the Invoke operation (reached max retries: 1): Rate Exceeded.

```

## Log
- `05:09:40` PASS  G0_checker_scheduled — rules=['justhodl-outcome-checker-4h', 'justhodl-outcome-checker-daily'] schedules=[] created=None
- `05:10:06` PASS  G1_normalizer_deployed — marker in zip
