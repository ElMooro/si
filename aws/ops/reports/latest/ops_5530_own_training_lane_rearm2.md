# ops 5530 (re-arm of 5528/5529) -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip

**Status:** success  
**Duration:** 1.7s  
**Finished:** 2026-09-13T21:15:39+00:00  

## Data

| head |
|---|
| f2fc74cd4a |

## Log
- `21:15:38` ✅ justhodl-ai receipt commit=0cfdd34 source_identical=True sha_match=True run=34782830752
- `21:15:38` ⚠ ECR unavailable to the runner user (An error occurred (LimitExceeded) when calling the AttachUserPolicy operation: Cannot exceed quota for PoliciesPerUser: 10); mirror workflow deferred -- pin uses the AWS DLC by tag
- `21:15:39` ✅ recipe bundled + pinned (image by tag until the mirror workflow pins the digest)
- `21:15:39` ✗ no live processing price for ml.m5.xlarge -- refusing unpriced spend
- `21:15:39` ✗ RED -- price
