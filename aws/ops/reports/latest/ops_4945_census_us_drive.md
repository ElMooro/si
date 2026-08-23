## G0 walker state

**Status:** success  
**Duration:** 182.6s  
**Finished:** 2026-08-23T14:35:52+00:00  

## Data

| card_keys | card_mb | min_first_year | n_done | n_total | phase | rows_total | universe |
|---|---|---|---|---|---|---|---|
| 44 | None | 1 | 21 | 21 | COMPLETE | 2082816 | 94 |

## Log
- `14:32:49` G0 PASS phase=COMPLETE n_total=21 n_done=21 rows=2082816 updated=2026-08-23T14:28:43+00:00
## G1 catalog

- `14:32:49` G1 PASS n_total=21 catalog=21 universe=94
## G2 drain drive (async, no sync invokes)

- `14:32:49`   t+   0s phase=COMPLETE n_done=21/21 rows=2082816 q=0
- `14:32:49` G2 PASS phase=COMPLETE n_done=21/21 rows_total=2082816 min_first_year=1 kicks=0 failures=[]
## inception coverage (top datasets by rows)

- `14:32:49`   m3                                 full_for  rows=600278   1..1
- `14:32:49`   qfr                                full      rows=378854   2000..2026
- `14:32:49`   mrts                               full      rows=195161   1992..2026
- `14:32:49`   mwts                               full      rows=157052   1992..2026
- `14:32:49`   qss                                full      rows=151236   2003..2026
- `14:32:49`   advm3                              full_for  rows=132240   1..1
- `14:32:49`   bfs                                full_for  rows=116256   1..1
- `14:32:49`   resconst                           full      rows=90361    1959..2026
- `14:32:49`   vip                                full      rows=83904    2002..2026
- `14:32:49`   marts                              full      rows=58730    1992..2026
- `14:32:49`   ressales                           full      rows=28416    1963..2026
- `14:32:49`   mtis                               full      rows=23816    1992..2026
- `14:32:49`   mhs                                full      rows=22140    1959..2014
- `14:32:49`   hv                                 full      rows=12240    1956..2026
- `14:32:49`   mrtsadv                            full      rows=8598     1992..2026
- `14:32:49`   mwtsadv                            full      rows=8598     1992..2026
- `14:32:49`   qpr                                full      rows=5830     1968..2026
- `14:32:49`   mhs2                               full_for  rows=3678     1..1
- `14:32:49`   ftdadv                             full      rows=2484     1992..2026
- `14:32:49`   ftd                                full      rows=2472     1992..2026
- `14:32:49`   qtax                               full_for  rows=472      1..1
## G3 provider-catalog card

- `14:35:31` G3 PASS card={"slug": "census-us", "name": "US Census Bureau", "api": "api.census.gov/data/timeseries", "datasets": 44, "datasets_target": null, "coverage_pct": null, "coverage_note": null, "coverage_basis": null, "denied_source_side": null, "unit": "keys", "n_keys": 44, "total_mb": 11.36, "hot_feeds": 0, "serie
## G4 sentinel pipeline

- `14:35:51` G4 PASS pipeline={'name': 'census-us', 'status': 'COMPLETE', 'detail': 'COMPLETE 21/21 datasets · 2082816 rows', 'age_min': 0.6}
## G5 served surfaces

- `14:35:52` G5 PASS origin=True(200) edge=True(200) page_generic=True(200)
- `14:35:52` ops 4945 GREEN -- US Census Bureau importing full history since inception; card live on data.html; sentinel + 15-min Scheduler finish and refresh autonomously
