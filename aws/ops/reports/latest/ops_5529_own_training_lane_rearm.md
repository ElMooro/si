# ops 5529 (re-arm of 5528) -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip

**Status:** success  
**Duration:** 2.1s  
**Finished:** 2026-09-13T21:13:11+00:00  

## Data

| head |
|---|
| 5edf202743 |

## Log
- `21:13:09` ✅ justhodl-ai receipt commit=0cfdd34 source_identical=True sha_match=True run=34782830752
- `21:13:10` ⚠ ECR unavailable to the runner user (An error occurred (LimitExceeded) when calling the PutUserPolicy operation: Maximum policy size of 2048 bytes exceeded for user github-actio); mirror workflow deferred -- pin uses the AWS DLC by tag
- `21:13:10` ✅ recipe bundled + pinned (image by tag until the mirror workflow pins the digest)
- `21:13:11` ✗ no live processing price for ml.m5.xlarge -- refusing unpriced spend
- `21:13:11` ✗ RED -- price
