
**Status:** failure  
**Duration:** 17.4s  
**Finished:** 2026-09-19T22:10:55+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5877_holdings_recurring_acceptance.py", line 69, in main
    schedule = scheduler.get_schedule(Name='justhodl-holdings-originals-research', GroupName='default')
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceNotFoundException: An error occurred (ResourceNotFoundException) when calling the GetSchedule operation: Schedule justhodl-holdings-originals-research does not exist.

```

## Data

| action | invocation_origin | legacy_collector_invoked | runtime |
|---|---|---|---|
| holdings_research_collect | AWS EventBridge Scheduler | False | {'commit': '42b290f582b4a71bdc9cb7bcb060416efaa412ac', 'code_sha256': 's8nKFUkJ/FrK+GS3ct0noATLDHmOZfgXJGH9m7z5V7k='} |

## Log

