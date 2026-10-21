# ops 6595 - release the wl-series-api anomaly hold (cause removed from the served site)

**Status:** success  
**Duration:** 8.5s  
**Finished:** 2026-10-21T13:32:41+00:00  

## Data

| beacon_gone | concurrency_after | released |
|---|---|---|
| True | 2 | True |

## Log
- `13:32:32` served jh-nav-drawer.js: 31630 bytes, beacon present: False, ops 6592 note present: True
- `13:32:35` before: anomaly_held [["justhodl-wl-series-api", "2026-10-21T19:07:37.810878+00:00"]]; concurrency 0
- `13:32:35` release_anomaly: {"released": [["justhodl-wl-series-api", "restored"]]}
- `13:32:41` after: anomaly_held []; concurrency 2
- `13:32:41` ✅ justhodl-wl-series-api released; reserved concurrency 2; the beacon is gone from the served site
