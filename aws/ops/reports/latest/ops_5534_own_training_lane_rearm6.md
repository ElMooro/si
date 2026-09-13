# ops 5534 (re-arm of 5528-5533) -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip

**Status:** failure  
**Duration:** 46.4s  
**Finished:** 2026-09-13T21:36:08+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5534_own_training_lane_rearm6.py", line 262, in main
    sm.create_processing_job(
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (AccessDeniedException) when calling the CreateProcessingJob operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: sagemaker:AddTags on resource: arn:aws:sagemaker:us-east-1:857687956942:processing-job/jh-stage-qwen2-5-coder-7b-ins-20260913-213608 because no identity-based policy allows the sagemaker:AddTags action

```

## Data

| head |
|---|
| 4f3fdafda5 |

## Log
- `21:35:22` ✅ justhodl-ai receipt commit=bea299a source_identical=True sha_match=True run=34783469659
- `21:35:22` ✅ ECR justhodl/factory-train exists
- `21:35:22` ✅ recipe bundled + pinned (image by tag until the mirror workflow pins the digest)
- `21:35:23` ✅ pricing + processing-job (jh-stage-*) policies attached to group justhodl-runner-ecr for github-actions-justhodl; waiting 45 s
- `21:36:08` ✅ pricing cache: 0 cached misses purged ()
- `21:36:08` ✅ processing price ml.m5.xlarge = $0.2300/h (source aws-price-list); job cap $0.4600 at 7200s
