# ops 5548 (burst 1) -- first owned trace burst: prompts-only tasks, runner rights, live price, spot job on the digest-pinned image

**Status:** success  
**Duration:** 154.1s  
**Finished:** 2026-09-14T02:22:21+00:00  

## Data

| burst | cap_usd | head | tasks |
|---|---|---|---|
|  |  | 31ff58a26b |  |
| jh-burst-gen0b1-20260914-022049 | 3.03 |  | 570 |

## Log
- `02:20:49` ✅ owner task cards riding this burst: 0
- `02:20:50` ✅ tasks: 570 prompts (holdout excluded 164, scanned 969) families={"mbpp": 464, "apps-introductory": 106} -> s3://justhodl-ai-857687956942/factory/bursts/tasks/20260914-022049/
- `02:20:50` ✅ training-job rights attached to group justhodl-runner-ecr for github-actions-justhodl; waiting 90 s
- `02:22:21` ✅ training price ml.g5.2xlarge = $1.5150/h (aws-price-list); burst cap $3.0300 at 7200s (spot bills less)
- `02:22:21` ✅ burst jh-burst-gen0b1-20260914-022049 launched: 570 tasks x K=4 at T=0.8 on ml.g5.2xlarge spot, image sha256:39b1be47f293 (cap $3.0300)
- `02:22:21` ✅ GREEN -- first burst running; on Completed, dispatch factory-trace-verify.yml (burst=jh-burst-gen0b1-20260914-022049) to keep only passes
