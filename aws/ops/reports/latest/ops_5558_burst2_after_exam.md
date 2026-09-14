# ops 5558 -- burst 2 (MBPP+APPS, K=6) after the base exam job frees the single spot g5.2xlarge

**Status:** success  
**Duration:** 253.8s  
**Finished:** 2026-09-14T21:47:56+00:00  

## Data

| burst_job | cap_usd | head |
|---|---|---|
|  |  | e99fc9f280 |
| jh-burst-gen0b2-20260914-214444 | 3.03 |  |

## Log
- `21:43:43` quota busy: jh-exam-gen0-20260914-213415; waiting
- `21:44:44` ✅ spot g5.2xlarge free
- `21:47:56` ✅ BURST 2 launched: jh-burst-gen0b2-20260914-214444 -- 570 prompts x K=6 (holdout excluded 164); verify with factory-trace-verify.yml all_samples=true
- `21:47:56` ✅ GREEN -- burst 2 running on the owned lane (quota serialized)
