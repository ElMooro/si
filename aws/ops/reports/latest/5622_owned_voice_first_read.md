# ops 5622 -- the market read through the owned model

**Status:** failure  
**Duration:** 802.8s  
**Finished:** 2026-09-17T18:46:09+00:00  

## Error

```
SystemExit: 1
```

## Data

| deterministic | elapsed_s | llm_path | origin | owned_error | owned_pending | owned_state | read_id |
|---|---|---|---|---|---|---|---|
| True | 50.9 | governed-router | glm: glm failed: HTTP Error 429: Too Many Requests | owned:qwen2-5-coder-7b-instruct@c03e6d358207 | None | req-c45319b36c7e70ac5c9e5e5f | queued | 20260917T183337Z |

## Log
## 1. Force a governed read

## 2. Tick until the owned answer settles (endpoint may be waking from zero)

- `18:34:54` t+ 1 min  owned=running  endpoint=InService instances=0  tick=done
- `18:36:02` t+ 2 min  owned=running  endpoint=InService instances=0  tick=done
- `18:37:09` t+ 3 min  owned=running  endpoint=InService instances=0  tick=done
- `18:38:16` t+ 4 min  owned=running  endpoint=InService instances=0  tick=done
- `18:39:24` t+ 5 min  owned=running  endpoint=Updating instances=0  tick=done
- `18:40:32` t+ 6 min  owned=running  endpoint=Updating instances=0  tick=done
- `18:41:40` t+ 7 min  owned=running  endpoint=Updating instances=0  tick=done
- `18:42:47` t+ 8 min  owned=running  endpoint=Updating instances=0  tick=done
- `18:43:54` t+ 9 min  owned=running  endpoint=Updating instances=0  tick=done
- `18:45:02` t+10 min  owned=running  endpoint=Updating instances=0  tick=done
- `18:46:09` t+11 min  owned=failed  endpoint=Updating instances=0  tick=done
## 3. What the owned model said

- `18:46:09` ✗ owned voice failed: {'error': 'exception occurred during rolling batch inference', 'code': 424} | raw: None
