# ops 4334 -- the fingerprint becomes a tier

**Status:** failure  
**Duration:** 2.7s  
**Finished:** 2026-08-03T20:48:28+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4334_prime_convergence.py", line 74, in <module>
    pr = json.loads(s3.get_object(
                    ^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.NoSuchKey: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.

```

## Log
- `20:48:28` root: {"errorMessage": "name 's3' is not defined", "errorType": "NameError", "requestId": "ee88c82a-2dbf-4701-bf58-23cabf87fd13", "stackTrace": ["  File \"/var/task/l
- `20:48:28` AAPL: prime=None combo= archetype=None rc=None pct_all=None
- `20:48:28` GOOGL: prime=None combo= archetype=None rc=None pct_all=None
- `20:48:28` MSFT: prime=None combo= archetype=None rc=None pct_all=None
- `20:48:28` ✅ ORCL correctly not prime
