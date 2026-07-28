# ops 4017 — public ACL on the v1.5.0 zip

**Status:** failure  
**Duration:** 0.2s  
**Finished:** 2026-07-28T13:34:50+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/pending/ops_4017_zip_acl.py", line 31, in main
    s3.put_object_acl(Bucket=BUCKET, Key=KEY, ACL="public-read")
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (AccessControlListNotSupported) when calling the PutObjectAcl operation: The bucket does not allow ACLs

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4017_zip_acl.py", line 35, in main
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=body,
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (AccessControlListNotSupported) when calling the PutObject operation: The bucket does not allow ACLs

```

## Data

| s3_bytes | version |
|---|---|
| 14324 | 1.5.0 |

## Log
- `13:34:50`   put_object_acl: ClientError — re-putting with ACL
