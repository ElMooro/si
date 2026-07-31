# ops 4146 — v3.19.0 settle, fire, gate

**Status:** failure  
**Duration:** 555.8s  
**Finished:** 2026-07-31T01:06:40+00:00  

## Error

```
SystemExit: 1
```

## Data

| BM | CBBS | FER | GDPYY | INTR | IRYY | LG | M0 | UR | feed_err | src-bis | src-imf | src-wb | total_live |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  | None |  |  |  |  |
| 140 | 144 | 183 | 261 | 46 | 240 | 141 | 143 | 234 |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 21 | 266 | 444 | 3349 |

## Log
## A. feed v1.3: settle + invoke + counts

- `00:57:59` ✅   update accepted (attempt 0)
- `00:57:59`   [0] ('Active', 'InProgress', 'The function is being created.')
- `00:58:18` ✅   settled at loop 2
- `01:06:40` ✅   artifact v3.19.0 after ~495s
- `01:06:40`   demand LG: watched=25 served=17
- `01:06:40`   demand CBBS: watched=89 served=61
- `01:06:40`   demand M0: watched=102 served=78
- `01:06:40`   demand BM: watched=2 served=2
- `01:06:40`   spot BRLG: LIVE v=8861998144914.4 src=imf:MFS_DC (family)
- `01:06:40`   spot BRCBBS: LIVE v=5027289480000.0 src=imf:MFS_CBS TA (family)
- `01:06:40`   spot JPM0: LIVE v=582652600000000.0 src=imf:MFS_CBS MB (family)
- `01:06:40`   spot BRBM: None v=None src=None
- `01:06:40` ✅   total LIVE >= 3300
- `01:06:40` ✅   BM feed >= 100
- `01:06:40` ✅   LG demand-served >=50%
- `01:06:40` ✅   CBBS demand-served >=50%
- `01:06:40` ✅   M0 demand-served >=50%
- `01:06:40` ✅   BM demand-served >=50%
- `01:06:40` ✅   spot BRLG LIVE&plausible
- `01:06:40` ✅   spot BRCBBS LIVE&plausible
- `01:06:40` ✅   spot JPM0 LIVE&plausible
- `01:06:40` ✗   spot BRBM LIVE&plausible
- `01:06:40` ✗ FAILED: ['spot BRBM LIVE&plausible']
