# ops 4691 — real browser CORS: diagnose + fix

**Status:** failure  
**Duration:** 0.5s  
**Finished:** 2026-08-15T02:55:00+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4691_cors_fix.py", line 95, in main
    lam.update_function_url_config(
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the UpdateFunctionUrlConfig operation: 1 validation error detected: Value '[POST, OPTIONS, GET]' at 'cors.allowMethods' failed to satisfy constraint: Member must satisfy constraint: [Member must have length less than or equal to 6, Member must have length greater than or equal to 0, Member must satisfy regular expression pattern: .*, Member must not be null]

```

## Log
## 1. Current Function URL config (the AWS-managed policy, not the code's own headers)

- `02:54:59`   URL: https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws/
- `02:54:59`   AuthType: NONE
- `02:54:59`   Cors (AWS-enforced, pre-empts the Lambda code): {'AllowCredentials': False, 'AllowHeaders': ['content-type'], 'AllowMethods': ['*'], 'AllowOrigins': ['*'], 'MaxAge': 86400}
- `02:54:59` ✅   [auth] AuthType=NONE (browser calls need this — IAM auth would block every unsigned browser request)
## 2. REAL preflight — exactly what Khalid's browser sent

- `02:55:00`   OPTIONS status: 200
- `02:55:00`   response headers: {'Date': 'Sat, 15 Aug 2026 02:55:00 GMT', 'Content-Type': 'application/json', 'Content-Length': '0', 'Connection': 'close', 'x-amzn-RequestId': '959a7690-1137-4d26-92b4-a1e60c5dbb8a', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'content-type', 'Access-Control-Allow-Methods': '*', 'Access-Control-Max-Age': '86400'}
- `02:55:00`   VERDICT: preflight WOULD BE REJECTED by the browser — this is the 'Failed to fetch' cause
## 3. Fix: set the Function URL's own Cors policy

