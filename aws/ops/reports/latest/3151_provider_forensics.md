# ops 3151 — LLM provider forensics

**Status:** failure  
**Duration:** 68.1s  
**Finished:** 2026-07-12T06:04:51+00:00  

## Error

```
SystemExit: 1
```

## Data

| anthropic_live | glm_ok | n_fails | n_warns | rich | row_errors | theses | verdict |
|---|---|---|---|---|---|---|---|
| None | True |  |  |  |  |  |  |
|  |  |  |  | 0 | 15 | 15 |  |
|  |  | 1 | 0 |  |  |  | FAIL |

## Log
## 1. CW verbatim from the 3150 run

- `06:03:43` CW: [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback
- `06:03:43` CW: [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback
- `06:03:43` CW: [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback
- `06:03:43` CW: [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback
- `06:03:43` CW: [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback
- `06:03:43` CW: [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback
## 2. Provider probes (runner-side, verbatim)

- `06:03:46` GLM probe: HTTP 200 · {"choices":[{"finish_reason":"length","index":0,"message":{"content":"","reasoning_content":"1.  **Analyze the Request:** The user is asking me to reply with the exact string","rol
- `06:03:46` ANTHROPIC_API_KEY: runner secret empty
- `06:03:46` ANTHROPIC_API_KEY_NEW: HTTP 400 · {"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to u
## 3. Conditional fix + gate

- `06:03:50` cold invoke fired
- `06:04:51` error sample: {"symbol": "NVDA", "error": "empty", "raw": null}
- `06:04:51` ✗ 0 rich with a proven-live provider — residual is engine-side; sample above
