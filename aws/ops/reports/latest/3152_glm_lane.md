# ops 3152 — policy vs revert vs GLM trap

**Status:** failure  
**Duration:** 70.4s  
**Finished:** 2026-07-12T06:08:28+00:00  

## Error

```
SystemExit: 1
```

## Data

| budget | engine_wall_s | mode_now | n_fails | n_warns | rich | row_errors | theses | verdict |
|---|---|---|---|---|---|---|---|---|
| 8 |  | normal |  |  |  |  |  |  |
|  | 1.1 |  |  |  | 0 | 15 | 15 |  |
|  |  |  | 1 | 0 |  |  |  | FAIL |

## Log
## 1. Governance NOW

## 2. CW tail of the LAST invoke (unfiltered)

- `06:07:18` CW: [llm_router] ALL providers down (TimeoutError('claude circuit open')) -> empty; engine uses deterministic fallback
- `06:07:18` CW: [WARNING]	2026-07-12T05:59:17.249Z	7d5888a8-e483-4ca4-a386-72a25bfa8b34	claude_fail symbol=V err=empty
- `06:07:18` CW: [llm_router] GLM failed (TimeoutError('glm circuit open')); falling back to Haiku (cost-safe — NOT Sonnet)
- `06:07:18` CW: [llm_router] ALL providers down (TimeoutError('claude circuit open')) -> empty; engine uses deterministic fallback
- `06:07:18` CW: [WARNING]	2026-07-12T05:59:17.255Z	7d5888a8-e483-4ca4-a386-72a25bfa8b34	claude_fail symbol=ADBE err=empty
- `06:07:18` CW: [llm_router] GLM failed (TimeoutError('glm circuit open')); falling back to Haiku (cost-safe — NOT Sonnet)
- `06:07:18` CW: [llm_router] ALL providers down (TimeoutError('claude circuit open')) -> empty; engine uses deterministic fallback
- `06:07:18` CW: [llm_router] GLM failed (TimeoutError('glm circuit open')); falling back to Haiku (cost-safe — NOT Sonnet)
- `06:07:18` CW: [llm_router] ALL providers down (TimeoutError('claude circuit open')) -> empty; engine uses deterministic fallback
- `06:07:18` CW: [WARNING]	2026-07-12T05:59:18.775Z	7d5888a8-e483-4ca4-a386-72a25bfa8b34	claude_fail symbol=MSFT err=empty
## 3. Cold invoke, patient poll

- `06:08:28` error sample: {"symbol": "NVDA", "error": "empty", "raw": null}
## 4. Restore FinOps policy

- `06:08:28` mode restored to on_demand (ops-2891 policy). Re-enabling background LLM fleet-wide is Khalid's switch: aws ssm put-parameter --name /justhodl/llm/mode --value normal --overwrite  (premortem ≈15 GLM calls/weekday ≈ $0.05/day at current pricing; daily budget cap $8 stays)
- `06:08:28` ✗ 0 rich · wall 1s — CW above names the layer
