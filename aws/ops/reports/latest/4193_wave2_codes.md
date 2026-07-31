# ops 4193 — MPRYY config + cot codes-wide + wave-2

**Status:** failure  
**Duration:** 110.4s  
**Finished:** 2026-07-31T21:14:04+00:00  

## Error

```
SystemExit: 1
```

## Data

| MPRYY | cot_resolved | cot_wanted | mpryy_countries |
|---|---|---|---|
|  |  |  | 71 |
| 70 |  |  |  |
|  | 261 | 318 |  |

## Log
- `21:12:18` ✅   MPRYY def PUT (71 countries)
- `21:12:37` ✅   justhodl-cot-feed settled at loop 2
- `21:12:48` ✅   justhodl-tradingview settled at loop 1
- `21:14:04` ✅   vault fired — 4194 converts
- `21:14:04` ✅   MPRYY proven >= 40
- `21:14:04` ✅   cot v1.3 settled
- `21:14:04` ✅   vault v3.26.2 settled
- `21:14:04` ✅   MPRYY feed >= 40
- `21:14:04` ✗   cot wanted >= 330 (wide codes admitted)
- `21:14:04` ✅   cot resolved >= 260
- `21:14:04` ✗ FAILED: ['cot wanted >= 330 (wide codes admitted)']
