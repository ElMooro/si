# ops 3935 — v3.1 source-memory deploy

**Status:** success  
**Duration:** 258.2s  
**Finished:** 2026-07-26T23:09:47+00:00  

## Data

| n_resolved_via | run2_fred | run2_live | statuses |
|---|---|---|---|
| 344 | 104 | 443 | {'META': 1, 'LIVE': 443, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 115} |

## Log
- `23:05:30` ✅   settled attempt 1
- `23:08:17` ✅   run1 (populate resolved_via): artifact refreshed ~165s (fred_calls 122, cached 215, live 443)
- `23:09:47` ✅   run2 (source-memory fast path): artifact refreshed ~90s (fred_calls 104, cached 215, live 443)
- `23:09:47`   EU03Y: LIVE value=2.8518 via=ecb:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3Y
- `23:09:47`   DE02Y: LIVE value=2.8006 via=ecb:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y
- `23:09:47`   EUCA: LIVE value=28898.3886 via=None
- `23:09:47` ✅   v3.1 settled
- `23:09:47` ✅   run1 wrote
- `23:09:47` ✅   run2 wrote
- `23:09:47` ✅   EU03Y LIVE
- `23:09:47` ✅   DE02Y LIVE
- `23:09:47` ✅   EUCA LIVE
- `23:09:47` ✅   run2 fred_calls < 170
- `23:09:47` ✅   n_live >= 440
- `23:09:47` ✅   resolved_via on >= 300 rows
- `23:09:47` ✅   zero bare UNRESOLVED
- `23:09:47` ✅ PASS_ALL — v3.1: run2 fred_calls 104 (was 395), 443 LIVE, source memory on 344 rows
