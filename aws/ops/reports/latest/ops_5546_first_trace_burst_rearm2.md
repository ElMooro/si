# ops 5546 (re-arm of 5545) -- first owned trace burst: prompts-only tasks, runner rights, live price, spot job on the digest-pinned image

**Status:** success  
**Duration:** 130.1s  
**Finished:** 2026-09-14T00:59:58+00:00  

## Data

| burst | cap_usd | head | tasks |
|---|---|---|---|
|  |  | 03505de0e6 |  |
| jh-burst-gen0-20260914-005825 | 3.03 |  | 464 |

## Log
- `00:58:26` ✅ tasks: 464 prompts (holdout excluded 164, scanned 464) families={"mbpp": 464} -> s3://justhodl-ai-857687956942/factory/bursts/tasks/20260914-005825/
- `00:58:26` ✅ training-job rights attached to group justhodl-runner-ecr for github-actions-justhodl; waiting 90 s
- `00:59:57` ✅ training price ml.g5.2xlarge = $1.5150/h (aws-price-list); burst cap $3.0300 at 7200s (spot bills less)
- `00:59:58` ✅ burst jh-burst-gen0-20260914-005825 launched: 464 tasks x K=4 at T=0.8 on ml.g5.2xlarge spot, image sha256:39b1be47f293 (cap $3.0300)
- `00:59:58` ✅ GREEN -- first burst running; on Completed, dispatch factory-trace-verify.yml (burst=jh-burst-gen0-20260914-005825) to keep only passes
