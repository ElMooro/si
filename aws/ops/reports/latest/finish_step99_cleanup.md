# Finish DDB cleanup + verify S3 lifecycle

**Status:** failure  
**Duration:** 12.4s  
**Finished:** 2026-04-25T02:01:19+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/pending/100_finish_step99_cleanup.py", line 102, in <module>
    resp = s3.get_bucket_lifecycle_configuration(Bucket="justhodl-dashboard-live")
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (NoSuchLifecycleConfiguration) when calling the GetBucketLifecycleConfiguration operation: The lifecycle configuration does not exist

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/100_finish_step99_cleanup.py", line 133, in <module>
    except s3.exceptions.NoSuchLifecycleConfiguration:
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/errorfactory.py", line 51, in __getattr__
    raise AttributeError(
AttributeError: <botocore.errorfactory.S3Exceptions object at 0x7fd87ffc2750> object has no attribute NoSuchLifecycleConfiguration. Valid exceptions are: AccessDenied, BucketAlreadyExists, BucketAlreadyOwnedByYou, EncryptionTypeMismatch, IdempotencyParameterMismatch, InvalidObjectState, InvalidRequest, InvalidWriteOffset, NoSuchBucket, NoSuchKey, NoSuchUpload, ObjectAlreadyInActiveTierError, ObjectNotInActiveTierError, TooManyParts

```

## Log
## A. Grant dynamodb:DeleteTable IAM perm

- `02:01:06` ✅   Attached DynamoDBManageTables to github-actions-justhodl
## B. Re-run DDB table deletes

- `02:01:16`   Total tables: 25
- `02:01:16`   Deletion candidates: 18
- `02:01:17` ✅     Deleted: APIKeys
- `02:01:17` ✅     Deleted: MacroMetrics
- `02:01:17` ✅     Deleted: OpenBBUsers
- `02:01:17` ✅     Deleted: WebSocketConnections
- `02:01:17` ✅     Deleted: agent-cache-table
- `02:01:17` ✅     Deleted: aiapi-market-metadata
- `02:01:17` ✅     Deleted: autonomous-ai-system-data
- `02:01:17` ✅     Deleted: autonomous-ai-tasks
- `02:01:17` ✅     Deleted: bls-data-857687956942-bls-minimal
- `02:01:17` ✅     Deleted: chatgpt-agent-audit-log
- `02:01:18` ✅     Deleted: chatgpt-agent-state
- `02:01:18` ✅     Deleted: chatgpt-state
- `02:01:18` ✅     Deleted: fed-liquidity-cache-v3
- `02:01:18` ✅     Deleted: justhodl-historical
- `02:01:18` ✅     Deleted: liquidity-indicators-v3
- `02:01:18` ✅     Deleted: liquidity-reversals-v3
- `02:01:18` ✅     Deleted: openbb-bls-data
- `02:01:18` ✅     Deleted: openbb-bls-data-857687956942
- `02:01:18` 
  Deleted: 18, Failed: 0
## C. Verify S3 archive/* → Glacier lifecycle rule

