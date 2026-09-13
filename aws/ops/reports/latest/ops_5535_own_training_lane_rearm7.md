# ops 5535 (re-arm of 5528-5534) -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip

**Status:** success  
**Duration:** 93.4s  
**Finished:** 2026-09-13T21:41:59+00:00  

## Data

| cap_usd | head | instance | job |
|---|---|---|---|
|  | 3a16ba66cc |  |  |
| 0.46 |  | ml.m5.xlarge | jh-stage-qwen2-5-coder-7b-ins-20260913-214158 |

## Log
- `21:40:26` ✅ justhodl-ai receipt commit=bea299a source_identical=True sha_match=True run=34783469659
- `21:40:26` ✅ ECR justhodl/factory-train exists
- `21:40:27` ✅ recipe bundled + pinned (image by tag until the mirror workflow pins the digest)
- `21:40:28` ✅ pricing + processing-job (jh-stage-*) policies attached to group justhodl-runner-ecr for github-actions-justhodl; waiting 45 s
- `21:41:58` ✅ pricing cache: 0 cached misses purged ()
- `21:41:58` ✅ processing price ml.m5.xlarge = $0.2300/h (source aws-price-list); job cap $0.4600 at 7200s
- `21:41:59` ✅ processing job jh-stage-qwen2-5-coder-7b-ins-20260913-214158 launched: Qwen/Qwen2.5-Coder-7B-Instruct@main -> s3://justhodl-ai-857687956942/factory/models/base/qwen2-5-coder-7b-instruct/<revision>/ (cap $0.4600)
- `21:41:59` ✅ GREEN -- owned lane staged: ECR repo, pinned recipe, weight staging job running (CPU, capped); Gear B untouched until 5529 validates the manifest
