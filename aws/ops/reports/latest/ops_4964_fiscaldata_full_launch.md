## G-1 markers-in-checkout

**Status:** success  
**Duration:** 812.6s  
**Finished:** 2026-08-24T02:39:21+00:00  

## Data

| banked | failures | invalid_named | phase | rows | valid |
|---|---|---|---|---|---|
| 19 | 0 | 3 | COMPLETE | 1035193 | 19 |

## Log
- `02:25:48`   ok justhodl-fiscaldata-full     'v1.0.0 ops4964'
- `02:25:48`   ok justhodl-provider-catalog    'fd-note-v2'
## G0 settle

- `02:25:49`   justhodl-fiscaldata-full settled (0s)
- `02:26:15`   justhodl-provider-catalog settled (26s)
- `02:26:15` G0 PASS
## G0b schedules

- `02:26:16`   created justhodl-fiscaldata-full-2h rate(2 hours)
- `02:26:16`   created justhodl-fiscaldata-full-weekly rate(7 days)
## G1 chain-drive (18min)

- `02:26:16`   t+   0s None banked=0 rows=0 q=0 valid=0 fail=0
- `02:26:42`   t+  25s DRAIN banked=0 rows=0 q=19 valid=19 fail=0
- `02:30:29`   t+ 253s DRAIN banked=2 rows=496315 q=17 valid=19 fail=0
- `02:31:45`   t+ 328s DRAIN banked=5 rows=634662 q=14 valid=19 fail=0
- `02:33:01`   t+ 404s DRAIN banked=7 rows=748535 q=12 valid=19 fail=0
- `02:33:26`   t+ 430s DRAIN banked=9 rows=778604 q=10 valid=19 fail=0
- `02:33:52`   t+ 455s DRAIN banked=11 rows=790268 q=8 valid=19 fail=0
- `02:36:23`   t+ 607s DRAIN banked=15 rows=1022105 q=4 valid=19 fail=0
- `02:36:48`   t+ 632s COMPLETE banked=19 rows=1035193 q=0 valid=19 fail=0
- `02:36:48` G1 PASS phase=COMPLETE banked=19 rows=1035193 valid=19
## G2 substance: auctions since 1979-11-15

- `02:36:49`   auctions rows=11090 first=1979-11-15
- `02:36:49` G2 PASS
## G3 card (post-mark)

- `02:39:21` G3 PASS note=FULL FiscalData warehouse (fiscaldata-full v1): 19 endpoints · 1035193 rows since inception (auctions 1979-) · phase COMPLETE · delta-checked refresh
- `02:39:21` ops 4964 GREEN -- FiscalData full warehouse live; delta refresh 2h + weekly redrain own it
