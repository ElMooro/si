# ops 6574 - warehouse archive: sealed bucket, replication of the whole live bucket, backfill, proof

**Status:** failure  
**Duration:** 1.6s  
**Finished:** 2026-10-20T23:38:02+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6574_warehouse_archive.py", line 242, in main
    seal_policy(r, s3)
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6574_warehouse_archive.py", line 96, in seal_policy
    s3.put_bucket_policy(Bucket=ARCHIVE, Policy=json.dumps(policy))
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (MalformedPolicy) when calling the PutBucketPolicy operation: Policy has invalid action

```

## Log
- `23:38:01` ✅ archive bucket created: justhodl-warehouse-archive-857687956942
- `23:38:02` archive lifecycle: keep 3 old copies, abort stray uploads after 7 days
- `23:38:02` archive: versioning Enabled, public access blocked, SSE-S3
- `23:38:02` replication role exists
- `23:38:02` ✅ replication live: justhodl-dashboard-live -> justhodl-warehouse-archive-857687956942 (STANDARD_IA), delete markers Disabled
