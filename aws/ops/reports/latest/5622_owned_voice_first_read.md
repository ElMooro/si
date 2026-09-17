# ops 5622 -- the market read through the owned model

**Status:** failure  
**Duration:** 130.3s  
**Finished:** 2026-09-17T18:52:05+00:00  

## Error

```
SystemExit: 1
```

## Data

| deterministic | elapsed_s | llm_path | origin | owned_error | owned_pending | owned_state | read_id |
|---|---|---|---|---|---|---|---|
| True | 53.2 | governed-router | glm: glm failed: HTTP Error 429: Too Many Requests | owned:qwen2-5-coder-7b-instruct@c03e6d358207 | None | req-4f13f4532a9979102e3142be | queued | 20260917T185049Z |

## Log
## 1. Force a governed read

## 2. Tick until the owned answer settles (endpoint may be waking from zero)

- `18:52:05` t+ 1 min  owned=malformed  endpoint=InService instances=1  tick=done
## 3. What the owned model said

- `18:52:05` ✗ owned voice malformed: validation: stocks.read must be a non-empty string | raw: ```json
{
  "overall": "The current market environment is characterized by a mixed regime with a risk-off posture, indicating caution among investors. The global business cycle is in a mild hawkish phase, suggesting potential economic headwinds. The bond market is in a calm regime, and the crisis co
