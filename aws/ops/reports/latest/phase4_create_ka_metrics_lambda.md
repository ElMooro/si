# Phase 4 — create justhodl-ka-metrics + dual-write S3

**Status:** failure  
**Duration:** 4.6s  
**Finished:** 2026-04-26T13:33:10+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/216_phase4_execute.py", line 118, in <module>
    url_resp = lam.create_function_url_config(
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the CreateFunctionUrlConfig operation: 1 validation error detected: Value '[GET, POST, OPTIONS]' at 'cors.allowMethods' failed to satisfy constraint: Member must satisfy constraint: [Member must have length less than or equal to 6, Member must have length greater than or equal to 0, Member must satisfy regular expression pattern: .*, Member must not be null]

```

## Log
## 1. Pre-flight checks

- `13:33:06`   ✅ justhodl-khalid-metrics exists, will copy from
- `13:33:06`   ✅ justhodl-ka-metrics does not exist — safe to create
## 2. Download old Lambda zip

- `13:33:06`   old zip: 7514B
- `13:33:06`   source: 369 lines, files: ['lambda_function.py']
## 3. Patch source for S3 dual-write

- `13:33:06`   legacy writes: 9  new ka writes: 3
## 4. Build new Lambda zip

- `13:33:06`   new zip: 7477B
## 5. Create justhodl-ka-metrics

- `13:33:07`   ✅ created
- `13:33:10`   ✅ state=Active
## 6. Create Function URL for justhodl-ka-metrics

