# ops 5533 (re-arm of 5528-5532) -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip

**Status:** failure  
**Duration:** 49.4s  
**Finished:** 2026-09-13T21:31:20+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5533_own_training_lane_rearm5.py", line 246, in main
    sm.create_processing_job(
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (AccessDeniedException) when calling the CreateProcessingJob operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: sagemaker:AddTags on resource: arn:aws:sagemaker:us-east-1:857687956942:processing-job/jh-stage-qwen2-5-coder-7b-ins-20260913-213120 because no identity-based policy allows the sagemaker:AddTags action

```

## Data

| head |
|---|
| da0959ca4f |

## Log
- `21:30:31` ✅ justhodl-ai receipt commit=bea299a source_identical=True sha_match=True run=34783469659
- `21:30:32` ✅ ECR justhodl/factory-train exists
- `21:30:32` ✅ recipe bundled + pinned (image by tag until the mirror workflow pins the digest)
- `21:30:33` ✅ pricing:GetProducts granted to github-actions-justhodl via group justhodl-runner-ecr; waiting 45 s for propagation
- `21:31:18` ✅ pricing cache: 0 cached misses purged ()
- `21:31:19` price probe instanceType=ml.m5.xlarge-Processing failed An error occurred (AccessDeniedException) when calling the GetProducts operation: User: arn:aws:iam:
- `21:31:19` ✅ priced by component=Processing: $0.2300/h {"instanceType": "ml.m5.xlarge-Processing", "usagetype": "USE1-Processing:ml.m5.xlarge", "component": "Processing"}
- `21:31:19` ✅ processing price ml.m5.xlarge = $0.2300/h (source aws-price-list:component=Processing); job cap $0.4600 at 7200s
