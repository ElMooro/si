# ops 6922 - the dollar-shortage document, first compile by the deployed engine, 2026-11-22T19:12+00:00

**Status:** success  
**Duration:** 5.7s  
**Finished:** 2026-11-22T19:12:19+00:00  

## Data

| bytes | failed_checks | measures | observations | seconds |
|---|---|---|---|---|
| 219119 | 0 | 3 | 10051 | 2.0 |

## Log
## 1. Guard

- `19:12:14` justhodl-ciss-stress updated 2026-11-22T19:11:18 | as in this checkout: {'dollar_shortage.py': True, 'world_history.py': True, 'sovereign_desk.py': True, 'lambda_function.py': True}
- `19:12:15` before: data/ciss-dollar-shortage.json does not exist yet
## 2. One invoke, the dollar-shortage document only

- `19:12:17` invoke: HTTP 200, function error None, 2.0s | {"dollar": {"published": true, "generated_at": "2026-11-22T19:12:16.504909+00:00", "desk_generated_at": "2026-11-22T16:39:59.412623+00:00", "coverage": {"measures": 3, "observations": 10051, "status": "measured"}, "latest": {"AUX_EURUSD": [1.099, "2026-11-20"], "AUX_FX_EA": [20.193, "2026-11-13"], "AUX_FX_NONEA": [521.892, "2026-11-13"]}, "seconds": 1.2}}
## 3. Read back

- `19:12:17` document 219119 bytes written 19:12:18 | contract ciss-dollar-shortage.v1 1.0.0 | generated 2026-11-22T19:12:16.504909+00:00 | stands on the desk document of 2026-11-22T16:39
- `19:12:17`   AUX_EURUSD    EUR/USD reference rate | 1.099 on 2026-11-20 | 7141 observations 1999-01-04 .. 2026-11-20 | high [1.599, '2008-07-15'] | low [0.8252, '2000-10-26']
- `19:12:17`   AUX_FX_EA     Claims on euro-area residents in foreign currency | 20.193 on 2026-11-13 | 1455 observations 1999-01-01 .. 2026-11-13 | high [248.854, '2008-12-05'] | low [5.255, '1999-01-08']
- `19:12:17`   AUX_FX_NONEA  Claims on non-euro-area residents in foreign currency | 521.892 on 2026-11-13 | 1455 observations 1999-01-01 .. 2026-11-13 | high [552.989, '2022-11-04'] | low [131.15, '2008-09-19']
- `19:12:17` against the desk document and its weekly history: 0 problems []
- `19:12:18` through the worker the Dollar desk reads: HTTP 200, allow-origin *, generated 2026-11-22T19:12:16.504909+00:00, same document: True
- `19:12:19` the warehouse, the desk document, its history and the world histories are as before: True
- `19:12:19` ✅ the deployed engine published the dollar-shortage document; it equals the desk document figure for figure and nothing else was touched
