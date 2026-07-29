# ops 4096 — force past the 27-day NO_FREE_SOURCE gate

**Status:** failure  
**Duration:** 934.4s  
**Finished:** 2026-07-29T21:36:30+00:00  

## Error

```
SystemExit: 1
```

## Data

| before_cot_live | before_live | before_rows |
|---|---|---|
| 0 | 1176 | 1358 |

## Log
## A. baseline

- `21:20:57`   rows 1358  LIVE 1176  cot aliases 65
- `21:20:57`   cot rows LIVE before: 0
## B. invoke with force=true (async, then poll)

- `21:20:57`   async accepted: 202
- `21:36:30` ✗ artifact never moved
