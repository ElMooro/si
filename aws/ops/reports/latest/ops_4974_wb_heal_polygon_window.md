## P1 worldbank heal

**Status:** failure  
**Duration:** 426.5s  
**Finished:** 2026-08-25T14:39:11+00:00  

## Error

```
SystemExit: 1
```

## Log
- `14:32:05`   frozen-state: banked=9200 q=20289 lease=1787668326s-ago failures=1 as_of=2026-08-24T04:14:35
- `14:32:05`     fail GD_WBL_MAR_LAW_DIVORCE: {"err": "HTTP 400", "tries": 3}
- `14:32:05`   schedule -2h CREATED (the missing restarter)
- `14:32:36`   t+ 30s banked=9200 q=20278
- `14:33:06`   t+ 60s banked=9200 q=20261
- `14:33:36`   t+ 90s banked=9200 q=20250
- `14:34:06`   t+120s banked=9200 q=20247
- `14:34:36`   t+150s banked=9200 q=20232
- `14:35:06`   t+180s banked=9200 q=20225
- `14:35:36`   t+210s banked=9200 q=20216
- `14:36:07`   t+241s banked=9200 q=20202
- `14:36:37`   t+271s banked=9200 q=20190
- `14:37:07`   t+301s banked=9200 q=20178
- `14:37:37`   t+331s banked=9200 q=20163
- `14:38:07`   t+361s banked=9200 q=20158
- `14:38:07`   P1 FAIL (drain still frozen)
## P2 gdelt v1 last-3

- `14:38:07`   v1=4986/4986 v1_gb=44.59 phase=V1
- `14:38:07`     v1-fail 2013.zip: HTTP 404
- `14:38:07`     v1-fail 20221110.export.CSV.zip: HTTP 404
- `14:38:07`     v1-fail 20230323.export.CSV.zip: HTTP 404
## P3 polygon entitled window

- `14:38:07`   grouped 2016-06-01 -> 403 results=0 {"status":"NOT_AUTHORIZED","request_id":"b1c06461be0d7b1dc3c2815b8237e7b9","message":"Atte
- `14:38:08`   grouped 2019-06-03 -> 403 results=0 {"status":"NOT_AUTHORIZED","request_id":"95f7b7db37a12df2227e13728e189464","message":"Atte
- `14:38:08`   grouped 2021-06-01 -> 403 results=0 {"status":"NOT_AUTHORIZED","request_id":"9dfe7d76a018aea3a86a23859c04f251","message":"Atte
- `14:38:09`   grouped 2022-06-01 -> 200 results=0 
- `14:38:10`   grouped 2023-06-01 -> 200 results=0 
- `14:38:11`   grouped 2024-01-02 -> 200 results=0 
- `14:38:11`   entitled-from ~ NONE
## P4 fiscaldata universe grows

- `14:38:41`   t+ 30s universe=19 banked=19 invalid=3
- `14:39:11`   t+ 60s universe=33 banked=19 invalid=8
- `14:39:11`   P4 PASS universe=33 (+14) invalid-named=8
- `14:39:11` ops 4974 RED: P1
