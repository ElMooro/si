# ops 4181 — GRES + old-crop

**Status:** failure  
**Duration:** 72.0s  
**Finished:** 2026-07-31T19:15:23+00:00  

## Error

```
SystemExit: 1
```

## Data

| GRES | cot_resolved | cot_wanted |
|---|---|---|
| 0 |  |  |
|  | 241 | 298 |

## Log
- `19:14:22` ✅   justhodl-families-feed settled at loop 1
- `19:14:32` ✅   justhodl-cot-feed settled at loop 1
- `19:14:43` ✅   justhodl-tradingview settled at loop 1
- `19:15:23` ✅   vault fired — 4182 converts
- `19:15:23` ✅   feed v1.7 settled
- `19:15:23` ✅   cot v1.2 settled
- `19:15:23` ✅   vault v3.25.0 settled
- `19:15:23` ✗   GRES >= 90
- `19:15:23` ✅   cot resolved >= 240
- `19:15:23` ✗ FAILED: ['GRES >= 90']
