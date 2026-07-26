# ops 3926 — v2.3 suffix-key close

**Status:** failure  
**Duration:** 112.0s  
**Finished:** 2026-07-26T21:39:36+00:00  

## Error

```
SystemExit: 1
```

## Data

| coverage_pct | n_live | statuses |
|---|---|---|
| 70.9 | 402 | {'META': 1, 'LIVE': 402, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 162} |

## Log
- `21:37:45` ✅   settled attempt 1
- `21:39:36`   ES10Y-TVC: LIVE value=3.421 src=fleetsum:data/euro-fragmentation.json
- `21:39:36`   FR10Y-TVC: LIVE value=3.68 src=fleetsum:data/euro-fragmentation.json
- `21:39:36` ✅   settled
- `21:39:36` ✅   ES10Y-TVC LIVE via fleetsum
- `21:39:36` ✅   FR10Y-TVC LIVE via fleetsum
- `21:39:36` ✗   LIVE >= 404
- `21:39:36` ✅   zero bare UNRESOLVED
- `21:39:36` ✗ FAILED: ['LIVE >= 404']
