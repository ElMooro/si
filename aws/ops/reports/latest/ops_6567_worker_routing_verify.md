# ops 6567 - worker routing verification (served pages + edge data fetches)

**Status:** success  
**Duration:** 6.8s  
**Finished:** 2026-10-20T21:41:35+00:00  

## Data

| data_urls_fetched | edge_hits_on_second_fetch | pages_checked | s3_host_data_reads_left | worker_data_reads |
|---|---|---|---|---|
| 6 | 6 | 8 | 0 | 8 |

## Log
- `21:41:28` accumulation.html: HTTP 200 in 253 ms, 37 KB; S3-host data reads 0, worker data reads 1
- `21:41:29` accuracy.html: HTTP 200 in 196 ms, 26 KB; S3-host data reads 0, worker data reads 1
- `21:41:29` allocator.html: HTTP 200 in 165 ms, 29 KB; S3-host data reads 0, worker data reads 1
- `21:41:29` alpha-council.html: HTTP 200 in 191 ms, 17 KB; S3-host data reads 0, worker data reads 1
- `21:41:29` analogs.html: HTTP 200 in 172 ms, 28 KB; S3-host data reads 0, worker data reads 1
- `21:41:29` auctions.html: HTTP 200 in 216 ms, 133 KB; S3-host data reads 0, worker data reads 1
- `21:41:30` accum-composite.html: HTTP 200 in 173 ms, 11 KB; S3-host data reads 0, worker data reads 1
- `21:41:30` activist-13d.html: HTTP 200 in 161 ms, 22 KB; S3-host data reads 0, worker data reads 1
- `21:41:30` bonds.html (byte-pinned, left on S3 by design): HTTP 200, S3-host data reads 3
- `21:41:31` /data/accumulation-radar.json: HTTP 200/200, cf-cache-status - -> HIT, application/json, 113 KB, 409/186 ms
- `21:41:31` /data/calibration-snapshot.json: HTTP 200/200, cf-cache-status - -> HIT, application/json, 151 KB, 543/181 ms
- `21:41:32` /data/allocator.json: HTTP 200/200, cf-cache-status - -> HIT, application/json, 5 KB, 368/117 ms
- `21:41:32` /data/alpha-council.json: HTTP 200/200, cf-cache-status - -> HIT, application/json, 148 KB, 427/161 ms
- `21:41:33` /data/historical-analogs.json: HTTP 200/200, cf-cache-status - -> HIT, application/json, 5 KB, 376/131 ms
- `21:41:35` /data/auction-desk.json: HTTP 200/200, cf-cache-status - -> HIT, application/json, 24457 KB, 1110/922 ms
- `21:41:35` ✅ served pages read data through the Worker and the edge serves repeats as HITs
