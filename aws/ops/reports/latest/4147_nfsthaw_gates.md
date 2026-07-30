# ops 4146 — v3.19.0 settle, fire, gate

**Status:** failure  
**Duration:** 619.4s  
**Finished:** 2026-07-30T17:26:54+00:00  

## Error

```
SystemExit: 1
```

## Data

| src-bis | src-imf | src-wb | total_live |
|---|---|---|---|
| 21 | 266 | 444 | 3344 |

## Log
- `17:16:35` ✅   update accepted (attempt 0)
- `17:16:36`   [0] ('Active', 'InProgress', 'The function is being created.')
- `17:16:45` ✅   settled at loop 1
- `17:26:54` ✅   artifact v3.19.0 after ~600s
- `17:26:54`   spot BRLG: LIVE v=8861998144914.4 src=imf:MFS_DC (family)
- `17:26:54`   spot BRCBBS: LIVE v=5027289480000.0 src=imf:MFS_CBS TA (family)
- `17:26:54`   spot JPM0: LIVE v=582652600000000.0 src=imf:MFS_CBS MB (family)
- `17:26:54` ✗   imf family-src >=300 (FER+LG+CBBS+M0)
- `17:26:54` ✅   total LIVE >= 3300
- `17:26:54` ✅   spot BRLG LIVE&plausible
- `17:26:54` ✅   spot BRCBBS LIVE&plausible
- `17:26:54` ✅   spot JPM0 LIVE&plausible
- `17:26:54` ✗ FAILED: ['imf family-src >=300 (FER+LG+CBBS+M0)']
