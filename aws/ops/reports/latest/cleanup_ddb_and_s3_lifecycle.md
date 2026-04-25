# Cleanup: empty DDB tables + S3 archive lifecycle

**Status:** failure  
**Duration:** 4.3s  
**Finished:** 2026-04-25T01:57:00+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/pending/99_cleanup_ddb_and_s3_lifecycle.py", line 128, in <module>
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
  File "/home/runner/work/si/si/aws/ops/pending/99_cleanup_ddb_and_s3_lifecycle.py", line 133, in <module>
    except s3.exceptions.NoSuchLifecycleConfiguration:
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/errorfactory.py", line 51, in __getattr__
    raise AttributeError(
AttributeError: <botocore.errorfactory.S3Exceptions object at 0x7f5e3e034ad0> object has no attribute NoSuchLifecycleConfiguration. Valid exceptions are: AccessDenied, BucketAlreadyExists, BucketAlreadyOwnedByYou, EncryptionTypeMismatch, IdempotencyParameterMismatch, InvalidObjectState, InvalidRequest, InvalidWriteOffset, NoSuchBucket, NoSuchKey, NoSuchUpload, ObjectAlreadyInActiveTierError, ObjectNotInActiveTierError, TooManyParts

```

## Log
## A. List all DDB tables, identify empty ones

- `01:56:58`   Total tables: 25
- `01:56:58`   Empty + not in KEEP set (deletion candidates): 18
- `01:56:58`     APIKeys                                             created=2025-06-15
- `01:56:58`     MacroMetrics                                        created=2025-09-24
- `01:56:58`     OpenBBUsers                                         created=2025-06-15
- `01:56:58`     WebSocketConnections                                created=2025-06-15
- `01:56:58`     agent-cache-table                                   created=2025-09-19
- `01:56:58`     aiapi-market-metadata                               created=2025-09-14
- `01:56:58`     autonomous-ai-system-data                           created=2025-09-30
- `01:56:58`     autonomous-ai-tasks                                 created=2025-09-30
- `01:56:58`     bls-data-857687956942-bls-minimal                   created=2025-08-17
- `01:56:58`     chatgpt-agent-audit-log                             created=2025-09-17
- `01:56:58`     chatgpt-agent-state                                 created=2025-09-16
- `01:56:58`     chatgpt-state                                       created=2025-09-16
- `01:56:58`     fed-liquidity-cache-v3                              created=2025-09-07
- `01:56:58`     justhodl-historical                                 created=2025-09-21
- `01:56:58`     liquidity-indicators-v3                             created=2025-09-14
- `01:56:58`     liquidity-reversals-v3                              created=2025-09-14
- `01:56:58`     openbb-bls-data                                     created=2025-08-17
- `01:56:58`     openbb-bls-data-857687956942                        created=2025-08-17
- `01:56:59` ✅   Snapshot saved to S3 _audit/ddb_pre_delete_*.json (rollback ready)
## A.2 Delete empty tables

- `01:56:59` ✗     APIKeys: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/APIKeys because no identity-based policy allows the dynamodb:DeleteTable action
- `01:56:59` ✗     MacroMetrics: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/MacroMetrics because no identity-based policy allows the dynamodb:DeleteTable action
- `01:56:59` ✗     OpenBBUsers: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/OpenBBUsers because no identity-based policy allows the dynamodb:DeleteTable action
- `01:56:59` ✗     WebSocketConnections: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/WebSocketConnections because no identity-based policy allows the dynamodb:DeleteTable action
- `01:56:59` ✗     agent-cache-table: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/agent-cache-table because no identity-based policy allows the dynamodb:DeleteTable action
- `01:56:59` ✗     aiapi-market-metadata: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/aiapi-market-metadata because no identity-based policy allows the dynamodb:DeleteTable action
- `01:56:59` ✗     autonomous-ai-system-data: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/autonomous-ai-system-data because no identity-based policy allows the dynamodb:DeleteTable action
- `01:56:59` ✗     autonomous-ai-tasks: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/autonomous-ai-tasks because no identity-based policy allows the dynamodb:DeleteTable action
- `01:56:59` ✗     bls-data-857687956942-bls-minimal: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/bls-data-857687956942-bls-minimal because no identity-based policy allows the dynamodb:DeleteTable action
- `01:56:59` ✗     chatgpt-agent-audit-log: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/chatgpt-agent-audit-log because no identity-based policy allows the dynamodb:DeleteTable action
- `01:57:00` ✗     chatgpt-agent-state: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/chatgpt-agent-state because no identity-based policy allows the dynamodb:DeleteTable action
- `01:57:00` ✗     chatgpt-state: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/chatgpt-state because no identity-based policy allows the dynamodb:DeleteTable action
- `01:57:00` ✗     fed-liquidity-cache-v3: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/fed-liquidity-cache-v3 because no identity-based policy allows the dynamodb:DeleteTable action
- `01:57:00` ✗     justhodl-historical: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/justhodl-historical because no identity-based policy allows the dynamodb:DeleteTable action
- `01:57:00` ✗     liquidity-indicators-v3: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/liquidity-indicators-v3 because no identity-based policy allows the dynamodb:DeleteTable action
- `01:57:00` ✗     liquidity-reversals-v3: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/liquidity-reversals-v3 because no identity-based policy allows the dynamodb:DeleteTable action
- `01:57:00` ✗     openbb-bls-data: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/openbb-bls-data because no identity-based policy allows the dynamodb:DeleteTable action
- `01:57:00` ✗     openbb-bls-data-857687956942: An error occurred (AccessDeniedException) when calling the DeleteTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DeleteTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/openbb-bls-data-857687956942 because no identity-based policy allows the dynamodb:DeleteTable action
- `01:57:00` 
  Deleted: 0, Failed: 18
## B. Add S3 lifecycle policy for archive/* → Glacier

