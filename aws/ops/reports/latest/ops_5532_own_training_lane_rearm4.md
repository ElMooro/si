# ops 5532 (re-arm of 5528-5531) -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip

**Status:** success  
**Duration:** 2.0s  
**Finished:** 2026-09-13T21:26:01+00:00  

## Data

| head |
|---|
| c2ec7c8275 |

## Log
- `21:25:59` ✅ justhodl-ai receipt commit=bea299a source_identical=True sha_match=True run=34783469659
- `21:26:00` ✅ ECR justhodl/factory-train created
- `21:26:00` ✅ recipe bundled + pinned (image by tag until the mirror workflow pins the digest)
- `21:26:00` ✅ pricing cache: 0 cached misses purged ()
- `21:26:00` price probe instanceType=ml.m5.xlarge-Processing failed An error occurred (AccessDeniedException) when calling the GetProducts operation: User: arn:aws:iam:
- `21:26:00` price probe component=Processing failed An error occurred (AccessDeniedException) when calling the GetProducts operation: User: arn:aws:iam:
- `21:26:01` price probe instanceType=ml.m5.xlarge-Training failed An error occurred (AccessDeniedException) when calling the GetProducts operation: User: arn:aws:iam:
- `21:26:01` price probe component=Training failed An error occurred (AccessDeniedException) when calling the GetProducts operation: User: arn:aws:iam:
- `21:26:01` ✗ no live processing price for ml.m5.xlarge -- refusing unpriced spend
- `21:26:01` ✗ RED -- price
