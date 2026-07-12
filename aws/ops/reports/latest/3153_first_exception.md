# ops 3153 — first GLM exception, verbatim

**Status:** failure  
**Duration:** 94.6s  
**Finished:** 2026-07-12T06:12:10+00:00  

## Error

```
SystemExit: 1
```

## Data

| doc_fresh | n_fails | n_warns | rich | verdict |
|---|---|---|---|---|
| True |  |  | 0 |  |
|  | 1 | 0 |  | FAIL |

## Log
- `06:10:43` cold invoke fired
## CW of THIS run

- `06:12:10` CW: [INFO]	2026-07-12T06:10:44.463Z	d2b5f768-0a0f-47ed-9a97-76d9860ea4b5	premortem-engine starting v1
- `06:12:10` CW: [llm_router] GLM failed (TimeoutError('The read operation timed out')); falling back to Haiku (cost-safe — NOT Sonnet)
- `06:12:10` CW: [llm_router] GLM failed (TimeoutError('The read operation timed out')); falling back to Haiku (cost-safe — NOT Sonnet)
- `06:12:10` CW: [llm_router] ALL providers down (<HTTPError 400: 'Bad Request'>) -> empty; engine uses deterministic fallback
- `06:12:10` CW: [llm_router] ALL providers down (<HTTPError 400: 'Bad Request'>) -> empty; engine uses deterministic fallback
- `06:12:10` CW: [llm_router] GLM failed (TimeoutError('glm circuit open')); falling back to Haiku (cost-safe — NOT Sonnet)
- `06:12:10` CW: [llm_router] ALL providers down (TimeoutError('claude circuit open')) -> empty; engine uses deterministic fallback
- `06:12:10` CW: [llm_router] GLM failed (TimeoutError('glm circuit open')); falling back to Haiku (cost-safe — NOT Sonnet)
- `06:12:10` ✅ ROOT: [llm_router] GLM failed (TimeoutError('The read operation timed out')); falling back to Haiku (cost-safe — NOT Sonnet)
## Class-specific fix

- `06:12:10` FinOps policy restored (on_demand). Owner switch to re-enable background LLM fleet-wide: /justhodl/llm/mode = normal
- `06:12:10` ✗ root exception above requires the next fix class
