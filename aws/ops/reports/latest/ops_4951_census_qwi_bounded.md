## P0 bounded-range probe (4950: 'requires a bounded date/time range')

**Status:** success  
**Duration:** 143.7s  
**Finished:** 2026-08-23T17:06:08+00:00  

## Data

| failures | n_done | n_total | qwi_states | rows_total | s3_keys | store_mb |
|---|---|---|---|---|---|---|
| 5 | 51 | 56 | 51,51,51 | 3909472 | 325 | 32.45 |

## Log
- `17:03:45`   L-bounded-years  HTTP 200 rows=100   [["Emp","HirA","Sep","EarnS","time","state"], [null,null,"375317",null,"2001-Q1","01"], ["1793105","416657","406550","2516","2001-Q2","01"], ["1803213
- `17:03:46`   M-bounded-qtrs   HTTP 200 rows=100   [["Emp","HirA","Sep","EarnS","time","state"], [null,null,"375317",null,"2001-Q1","01"], ["1793105","416657","406550","2516","2001-Q2","01"], ["1803213
- `17:03:47`   confirm state:48 HTTP 200 rows=124
- `17:03:47` P0 PASS winner=L-bounded-years (AL 100 / TX 124 rows) -> full_time='from 1990 to {cur}' written for qwi-sa,qwi-se,qwi-rh
## G0 zip-settle v1.1.3

- `17:03:47` G0 PASS after 0s
## G0b redo qwi x3

## G1 drive to COMPLETE

- `17:04:07`   t+   0s DRAIN done=48/56 rows=3892162 q=3 head=qwi-rh geo_i=None fail=5
- `17:04:47`   t+  40s DRAIN done=49/56 rows=3897932 q=2 head=qwi-se geo_i=None fail=5
- `17:05:08`   t+  60s DRAIN done=50/56 rows=3903702 q=1 head=qwi-sa geo_i=None fail=5
- `17:05:48`   t+ 100s COMPLETE done=51/56 rows=3909472 q=0 head=None geo_i=None fail=5
- `17:05:48` G1 PASS phase=COMPLETE 51+5==56 kicks=0
## G2 failures ledger

- `17:05:48`   FAIL aies-miscsector      no data any mode (last HTTP 400)
- `17:05:48`   FAIL asm-industry         no data any mode (last HTTP 400)
- `17:05:48`   FAIL poverty-saipe-schdist no data any mode (last HTTP 400)
- `17:05:48`   FAIL pseo-earnings        no data any mode (last HTTP 400)
- `17:05:48`   FAIL pseo-flows           no data any mode (last HTTP 400)
- `17:05:48` G2 PASS n=5 unexpected=[]
## G3 qwi inception

- `17:05:48`   qwi-rh   mode=geo_state rows=5770    states=51  1990..2025
- `17:05:48`   qwi-sa   mode=geo_state rows=5770    states=51  1990..2025
- `17:05:48`   qwi-se   mode=geo_state rows=5770    states=51  1990..2025
- `17:05:48` G3 PASS qwi_datasets=3
## G4 rows/bytes

- `17:05:48` G4 PASS rows=3909472 (+1826656 vs v1.0) 32.45MB keys=325
## G6 sentinel

- `17:06:08` G6 PASS {'name': 'census-us', 'status': 'COMPLETE', 'detail': 'COMPLETE 51/56 datasets · 3909472 rows · 5 source failures logged', 'age_min': 0.1}
- `17:06:08` ops 4951 GREEN -- qwi live per-state since the 1990s; the full timeseries universe is COMPLETE with exactly the five structurally out-of-ladder failures named
