# ops 4165 — convert the chewed queue

**Status:** failure  
**Duration:** 500.4s  
**Finished:** 2026-07-31T18:59:28+00:00  

## Error

```
SystemExit: 1
```

## Data

| honest_label_rows | total_live |
|---|---|
|  | 4555 |
| 0 |  |

## Log
- `18:51:18` ✅   justhodl-tradingview settled at loop 1
- `18:59:28` ✅   artifact after ~465s
- `18:59:28`   statuses: {"LIVE": 4555, "PENDING_RESOLUTION": 3113, "NO_FREE_SOURCE": 2388, "DISCONTINUED": 2, "META": 1}
- `18:59:28`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2369, "S&P Global PMI licensed": 8, "3M TB not in MOF JGB CSV \u2014 BOJ API next": 1, "referenced in eurodollar-plumbing code, not ": 1, "FTSE licensed": 1, "ChinaBond licensed": 1}
- `18:59:28` ✗ label wave inert: 0
