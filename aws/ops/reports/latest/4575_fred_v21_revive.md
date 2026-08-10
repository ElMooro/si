# ops 4575 — FRED priority-drain v2.1 revive

**Status:** failure  
**Duration:** 3.8s  
**Finished:** 2026-08-10T03:39:05+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4575_fred_v21_revive.py", line 201, in main
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

## Data

| cats_done | imported | key_source | lease_before | probe | rule_before | status_before | target_input_before |
|---|---|---|---|---|---|---|---|
|  |  | /justhodl/fred-api-key |  | OK |  |  |  |
|  |  |  |  |  | rate(5 minutes) |  | {"phase": "categories"} |
| 81 | 11927 |  | 0 |  |  | walking |  |

## Log
## 1. Settle the v2.1 deploy

- `03:39:01` ✅ settled: LastModified 2026-08-10T03:34:06.000+0000
## 2. Key probe (runner-side, one call — 403-incident rule)

## 3. Cron repair — existing rule, 15-min watchdog, explicit phase payload

- `03:39:03` ✅ rule → rate(15 minutes), target input → {"phase": "scoped_import"}
## 4. Un-wedge the lease

- `03:39:04` ✅ lease cleared
## 5. Kick one budgeted run (log tail captured)

