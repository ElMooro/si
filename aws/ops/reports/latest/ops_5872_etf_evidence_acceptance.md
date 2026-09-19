
**Status:** failure  
**Duration:** 78.5s  
**Finished:** 2026-09-19T20:04:08+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5872_etf_evidence_acceptance.py", line 78, in main
    scheduler=boto3.client('scheduler',region_name='us-east-1');schedule=scheduler.get_schedule(Name='justhodl-etf-true-flows-daily',GroupName='default')
                                                                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceNotFoundException: An error occurred (ResourceNotFoundException) when calling the GetSchedule operation: Schedule justhodl-etf-true-flows-daily does not exist.

```

## Data

| code_sha256 | collection_reinvoked | commit | prior_transport_failure |
|---|---|---|---|
| c9XriboytlZZbgmeOjRj3L01m507hM0vWSZH73eWxBw= | False | 619ccd108921c174705d92c22947fe56ed016e39 | ops5870 connection closed after publication |

## Log

