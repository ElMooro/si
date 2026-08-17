# 1. official-pulse birth

**Status:** failure  
**Duration:** 11.5s  
**Finished:** 2026-08-17T22:58:48+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4864_official_pulse_birth.py", line 202, in main
    bank = sread("data/providers/h41/WLRRAFOIAL.json")
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/ops_4864_official_pulse_birth.py", line 53, in sread
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
- `22:58:37` ✅ function Active + update settled
- `22:58:38` ✅ justhodl-official-pulse: token settled (attempt 1)
- `22:58:38` ✅ schedule Fri 09:00 UTC
- `22:58:48` ✅ justhodl-official-pulse fresh in 10s
# 2. pulse truths

- `22:58:48` ✗   rrp {} vs fred 2026-08-12/357392.0
