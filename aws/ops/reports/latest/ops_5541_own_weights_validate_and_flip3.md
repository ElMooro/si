# ops 5541 (re-arm of 5536/5538) -- owned base weights: wait for staging, validate manifest + objects, flip Gear B to model_source=own

**Status:** success  
**Duration:** 273.2s  
**Finished:** 2026-09-13T23:27:02+00:00  

## Data

| head | model_id |
|---|---|
| bd57efd694 | qwen2-5-coder-7b-instruct |

## Log
- `23:22:29` ✅ staging job from record: jh-stage-qwen2-5-coder-7b-ins-20260913-232228
- `23:27:02` ✗ manifest refused: unhashed or empty file in base manifest: .cache/huggingface/.gitignore.sagemaker-uploaded
- `23:27:02` ✗ RED -- manifest
