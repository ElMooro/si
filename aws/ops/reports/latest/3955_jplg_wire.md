# ops 3955 — JPLG comes home (vault v3.6 + gate v2.3)

**Status:** success  
**Duration:** 499.4s  
**Finished:** 2026-07-27T03:10:11+00:00  

## Data

| composite | coverage_pct | n_live | posture | statuses |
|---|---|---|---|---|
|  | 80.9 | 454 |  | {'META': 1, 'LIVE': 454, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 104} |
| -0.47 |  |  | RISK_OFF |  |

## Log
- `03:02:22` ✅   vault settled: 3
- `03:02:22` ✅   gate settled: 1
- `03:09:57` ✅   vault refreshed ~450s
- `03:09:57`   JPLG: LIVE value=7.07 prev=6.63 src=bank-of-japan asof=boj:202605 YoY
- `03:10:11`   gate jplg: OK value=7.07 adj=0.0
- `03:10:11` ✅   vault v3.6 settled
- `03:10:11` ✅   gate v2.3 settled
- `03:10:11` ✅   vault force wrote
- `03:10:11` ✅   JPLG LIVE via bank-of-japan
- `03:10:11` ✅   JPLG yoy plausible [-10,15]
- `03:10:11` ✅   JPLG asof 2026
- `03:10:11` ✅   n_live >= 454
- `03:10:11` ✅   zero bare UNRESOLVED
- `03:10:11` ✅   gate carry leg carries JPLG OK
- `03:10:11` ✅   posture valid
- `03:10:11` ✅ PASS_ALL — JPLG 7.07% (boj:202605 YoY) LIVE from the Bank of Japan and firing in the risk-gate carry leg (adj 0.0); vault 454 LIVE
