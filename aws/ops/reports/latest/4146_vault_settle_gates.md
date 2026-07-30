# ops 4146 — v3.19.0 settle, fire, gate

**Status:** failure  
**Duration:** 392.4s  
**Finished:** 2026-07-30T17:12:12+00:00  

## Error

```
SystemExit: 1
```

## Data

| src-bis | src-imf | src-wb | total_live |
|---|---|---|---|
| 17 | 261 | 443 | 3330 |

## Log
- `17:05:41` ✅   update accepted (attempt 0)
- `17:05:41`   [0] ('Active', 'InProgress', 'The function is being created.')
- `17:05:50` ✅   settled at loop 1
- `17:12:12` ✅   artifact v3.19.0 after ~375s
- `17:12:12`   spot JPLG: LIVE v=7.07 src=bank-of-japan
- `17:12:12`   spot BRCBBS: LIVE v=5027289480000.0 src=imf:MFS_CBS TA (family)
- `17:12:12`   spot JPM0: NO_FREE_SOURCE v=None src=unresolved_economics
- `17:12:12` ✗   imf family-src >=300 (FER+LG+CBBS+M0)
- `17:12:12` ✅   total LIVE >= 3300
- `17:12:12` ✗   spot JPLG LIVE&plausible
- `17:12:12` ✅   spot BRCBBS LIVE&plausible
- `17:12:12` ✗   spot JPM0 LIVE&plausible
- `17:12:12` ✗ FAILED: ['imf family-src >=300 (FER+LG+CBBS+M0)', 'spot JPLG LIVE&plausible', 'spot JPM0 LIVE&plausible']
