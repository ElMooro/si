# ops 4170 — IPYY + FI + label activation

**Status:** failure  
**Duration:** 115.4s  
**Finished:** 2026-07-31T18:15:14+00:00  

## Error

```
SystemExit: 1
```

## Data

| fi_bulk_countries | fi_mask | freq | tr_pick |
|---|---|---|---|
|  |  | M | POP_PCH_PA_PT |
| 146 | ..CP01.POP_PCH_PA_PT.M |  |  |

## Log
## A. IPYY: obs=13 yoy, retried, JPN->USA fallback

## B. FI: full-doc transform/freq sets, ranked pick

- `18:15:11`   attr order: ["COUNTRY", "INDEX_TYPE", "COICOP_1999", "TYPE_OF_TRANSFORMATION", "FREQUENCY"]
- `18:15:11`   transforms: ["IX", "POP_PCH_PA_PT", "SRP_IX", "SRP_POP_PCH_PA_PT", "SRP_YOY_PCH_PA_PT", "WGT", "WGT_PT", "YOY_PCH_PA_PT"]
- `18:15:11`   freqs: ["A", "M", "Q"]
- `18:15:14`   spots: [["USA", "0.1950197202776857"]]
- `18:15:14` ✗   IPYY computable
- `18:15:14` ✅   FI mask proven >=40
- `18:15:14` ✗ probes incomplete — not wiring
