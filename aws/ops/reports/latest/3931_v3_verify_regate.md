# ops 3929 — v3.0 async verify (Event-invoke + freshness)

**Status:** failure  
**Duration:** 303.7s  
**Finished:** 2026-07-26T22:20:11+00:00  

## Error

```
SystemExit: 1
```

## Data

| cached_1 | cached_2 | coverage_1 | fred_calls_1 | fred_calls_2 | marker | n_live_1 | n_live_2 | run1_artifact_age_min | statuses |
|---|---|---|---|---|---|---|---|---|---|
| 0 |  | 78.8 | 406 |  | tradingview-vault v3.0 CADENCE-AWARE | 447 |  | 9.0 |  |
|  |  |  |  |  |  |  |  |  | {'META': 1, 'LIVE': 447, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 117} |
|  | 447 |  |  | 182 |  |  | 447 |  |  |

## Log
- `22:15:08`   EUINTR: LIVE value=2.25 src=fred_alias:ECBDFR
- `22:15:08`   US03Y: LIVE value=4.4 src=fred_alias:DGS3
- `22:15:08`   GB10Y: LIVE value=4.796 src=fred_alias:IRLTLT01GBM156N
- `22:15:08`   USCLI: LIVE value=120 src=fleet:data/global-business-cycle.json
- `22:15:08`   10USNOTE: LIVE value=108.3281 src=yahoo:ZN=F
- `22:15:08`   NOVO_B: LIVE value=320.6 src=yahoo:NOVO-B.CO
## RUN 2 — Event-invoke, poll freshness, prove the cache

- `22:20:11` ✅   RUN 1 completed despite closed connection
- `22:20:11` ✅   EUINTR LIVE
- `22:20:11` ✅   US03Y LIVE
- `22:20:11` ✅   GB10Y LIVE
- `22:20:11` ✅   USCLI LIVE
- `22:20:11` ✅   10USNOTE LIVE
- `22:20:11` ✅   NOVO_B LIVE
- `22:20:11` ✗   n_live >= 450
- `22:20:11` ✅   zero UNRESOLVED
- `22:20:11` ✅   run2 artifact refreshed
- `22:20:11` ✗   run2 fred calls < 60 (cadence working)
- `22:20:11` ✅   run2 cached > 300
- `22:20:11` ✅   run2 n_live within tolerance of run1
- `22:20:11` ✗ FAILED: ['n_live >= 450', 'run2 fred calls < 60 (cadence working)']
