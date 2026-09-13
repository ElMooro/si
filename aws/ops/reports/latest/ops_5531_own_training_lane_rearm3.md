# ops 5531 (re-arm of 5528-5530) -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip

**Status:** success  
**Duration:** 22.6s  
**Finished:** 2026-09-13T21:20:48+00:00  

## Data

| head |
|---|
| bea299a719 |

## Log
- `21:20:26` ✅ justhodl-ai receipt commit=bea299a source_identical=True sha_match=True run=34783469659
- `21:20:47` ⚠ ECR unavailable to the runner user (An error occurred (AccessDeniedException) when calling the DescribeRepositories operation: User: arn:aws:iam::857687956942:user/github-actio); mirror workflow deferred -- pin uses the AWS DLC by tag
- `21:20:48` ✅ recipe bundled + pinned (image by tag until the mirror workflow pins the digest)
- `21:20:48` ✅ pricing cache: 2 cached misses purged (ml.m5.xlarge|training, ml.m5.xlarge|processing)
- `21:20:48` ✗ no live processing price for ml.m5.xlarge -- refusing unpriced spend
- `21:20:48` ✗ RED -- price
