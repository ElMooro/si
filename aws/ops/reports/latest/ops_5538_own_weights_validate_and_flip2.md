# ops 5538 (re-arm of 5536) -- owned base weights: wait for staging, validate manifest + objects, flip Gear B to model_source=own

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-09-13T23:11:53+00:00  

## Data

| head | model_id |
|---|---|
| 765107e150 | qwen2-5-coder-7b-instruct |

## Log
- `23:11:53` ✅ staging job from record: jh-stage-qwen2-5-coder-7b-ins-20260913-230824
- `23:11:53` ✅ job jh-stage-qwen2-5-coder-7b-ins-20260913-230824 -> Failed (AlgorithmError: , exit code: 2)
- `23:11:53` ✗ no manifest at factory/models/base/qwen2-5-coder-7b-instruct/manifest.json after 55 min (job status Failed)
- `23:11:53` ✗ RED -- manifest-missing
