# ops 5540 (restage 2) -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip

**Status:** success  
**Duration:** 93.2s  
**Finished:** 2026-09-13T23:22:29+00:00  

## Data

| cap_usd | head | instance | job |
|---|---|---|---|
|  | bd57efd694 |  |  |
| 0.46 |  | ml.m5.xlarge | jh-stage-qwen2-5-coder-7b-ins-20260913-232228 |

## Log
- `23:20:56` ✅ justhodl-ai receipt commit=bea299a source_identical=True sha_match=True run=34783469659
- `23:20:56` ✅ ECR justhodl/factory-train exists
- `23:20:57` ✅ recipe bundled + pinned (image by tag until the mirror workflow pins the digest)
- `23:20:58` ✅ pricing + processing-job (jh-stage-*) policies attached to group justhodl-runner-ecr for github-actions-justhodl; waiting 45 s
- `23:22:28` ✅ pricing cache: 0 cached misses purged ()
- `23:22:28` ✅ processing price ml.m5.xlarge = $0.2300/h (source aws-price-list); job cap $0.4600 at 7200s
- `23:22:28` log tail unavailable: An error occurred (InvalidParameterException) when calling the DescribeLogStreams operation: Cannot 
- `23:22:29` ✅ processing job jh-stage-qwen2-5-coder-7b-ins-20260913-232228 launched: Qwen/Qwen2.5-Coder-7B-Instruct@main -> s3://justhodl-ai-857687956942/factory/models/base/qwen2-5-coder-7b-instruct/<revision>/ (cap $0.4600)
- `23:22:29` ✅ GREEN -- owned lane staged: ECR repo, pinned recipe, weight staging job running (CPU, capped); Gear B untouched until 5529 validates the manifest
