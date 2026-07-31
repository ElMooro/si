# ops 4163 — vault wave + source-map + retire

**Status:** failure  
**Duration:** 512.9s  
**Finished:** 2026-07-31T15:55:07+00:00  

## Error

```
SystemExit: 1
```

## Data

| retired | source_map_n | total_live | tv_attributed |
|---|---|---|---|
|  |  | 4232 |  |
|  | 12103 |  | 2044 |
| [] |  |  |  |

## Log
- `15:46:45` ✅   justhodl-tradingview settled at loop 1
- `15:55:06` ✅   artifact after ~465s
- `15:55:07` ✅   vault v3.22.0 settled
- `15:55:07` ✗   total LIVE >= 4300
- `15:55:07` ✅   source-map >= 10000
- `15:55:07` ✗ FAILED: ['total LIVE >= 4300']
