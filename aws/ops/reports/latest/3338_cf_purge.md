- `03:59:32` ✗ purge FAILED (token lacks Zone>Cache Purge) — pivot to version-bump workaround
**Status:** failure  
**Duration:** 0.8s  
**Finished:** 2026-07-15T03:59:32+00:00  

## Error

```
SystemExit: 1
```

## Data

| RESULT | errors | errors2 | purge_all_ok | purge_all_status | purge_files_ok | purge_files_status | token_ok | token_present | token_suffix | token_verify_status | zone_id | zone_status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | True | 10d2 |  |  |  |
|  |  |  |  |  |  |  | True |  |  | 200 |  |  |
|  |  |  |  |  |  |  |  |  |  |  | fb59e2d0… | 200 |
|  | [{'code': 10000, 'message': 'Authentication error'}] |  |  |  | False | 401 |  |  |  |  |  |  |
|  |  | [{'code': 10000, 'message': 'Authentication error'}] | False | 401 |  |  |  |  |  |  |  |  |
| PURGE_DENIED |  |  |  |  |  |  |  |  |  |  |  |  |

## Log

