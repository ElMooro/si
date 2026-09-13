# ops 5536 -- owned base weights: wait for staging, validate manifest + objects, flip Gear B to model_source=own

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-09-13T23:01:28+00:00  

## Data

| head | model_id |
|---|---|
| b95912802d | qwen2-5-coder-7b-instruct |

## Log
- `23:01:28` ✅ staging job from record: jh-stage-qwen2-5-coder-7b-ins-20260913-214158
- `23:01:28` ✅ job jh-stage-qwen2-5-coder-7b-ins-20260913-214158 -> Failed (AlgorithmError: , exit code: 2)
- `23:01:28` ✗ no manifest at factory/models/base/qwen2-5-coder-7b-instruct/manifest.json after 50 min (job status Failed)
- `23:01:28` ✗ RED -- manifest-missing
