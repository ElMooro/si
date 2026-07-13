# ops 3261 — parity with the {note:{…}} wrapper

**Status:** failure  
**Duration:** 48.7s  
**Finished:** 2026-07-13T14:08:18+00:00  

## Error

```
SystemExit: 1
```

## Data

| in_brain | mirror | missing_before | n_fails | n_warns | put_failed | still_missing | upserted | verdict |
|---|---|---|---|---|---|---|---|---|
|  |  | 403 |  |  |  |  |  |  |
|  |  |  |  |  | 1 |  | 402 |  |
| 2949 | 3322 |  |  |  |  | 373 |  |  |
|  |  |  | 1 | 0 |  |  |  | FAIL |

## Log
- `14:08:11`     err: HTTP Error 400: Bad Request
- `14:08:18` ✗ 373 still absent
