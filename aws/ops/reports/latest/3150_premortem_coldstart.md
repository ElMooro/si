# ops 3150 — cold-start premortem, final gate

**Status:** failure  
**Duration:** 68.5s  
**Finished:** 2026-07-12T06:01:56+00:00  

## Error

```
SystemExit: 1
```

## Data

| elapsed_note | n_fails | n_warns | rich | row_errors | theses | verdict |
|---|---|---|---|---|---|---|
| doc at 2026-07-12T06:00:56.056651+00:00 |  |  | 0 | 15 | 15 |  |
|  | 1 | 0 |  |  |  | FAIL |

## Log
## 1. Container recycle (env nonce)

- `06:00:55` ✅ env nonce applied — next invoke is a cold container
## 2. Invoke + gate

- `06:01:56` error sample: {"symbol": "NVDA", "error": "empty", "raw": null}
- `06:01:56` ✗ still 0 rich after cold start — error sample above names the next layer
