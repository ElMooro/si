# ops 4108 — midpoint: progress + econ_probe verdict

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-07-30T02:14:07+00:00  

## Data

| attributed | delay_ms | distinct | done | economics_rows | elapsed_s | paused_s | pct | pct_universe | rate | recoveries | sc_err | sc_ok | total | wall_events |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 11200 |  | 4697 |  | 11691 | 0 | 46.4 |  | 24.1 | 67 | 82 | 4615 | 10116 | 0 |
| 465 |  | 30 |  | 0 |  |  |  | 4.5 |  |  |  |  |  |  |

## Log
## A. walk progress

## B. econ_probe — UNSLICED, the agency verdict

- `02:14:07`   [{"sym": "ECONOMICS:JPM3", "keys": ["description", "source", "source-description", "source-logoid", "source_description", "type"], "sample": "{\"description\":\"Japan Money Supply 
- `02:14:07`   M3\",\"source\":null,\"source-description\":null,\"source-logoid\":null,\"source_description\":null,\"type\":\"economic\"}"}, {"sym": "FRED:MABMM301JPM189S", "keys": [], "sample": 
- `02:14:07`   "null"}, {"sym": "ECONOMICS:JPCBBS", "keys": ["description", "source", "source-description", "source-logoid", "source_description", "type"], "sample": "{\"description\":\"Japan Cen
- `02:14:07`   tral Bank Balance Sheet\",\"source\":null,\"source-description\":null,\"source-logoid\":null,\"source_description\":null,\"type\":\"economic\"}"}, {"sym": "FRED:JPNASSETS", "keys":
- `02:14:07`    [], "sample": "null"}]
## C. coverage

- `02:14:07` ✅ MIDPOINT — 4697/10116 walked, 465 attributed, 0 economics, econ_probe PRESENT
