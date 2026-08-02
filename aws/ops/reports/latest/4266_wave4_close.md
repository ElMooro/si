# ops 4266 -- wave-4 closure

**Status:** success  
**Duration:** 45.0s  
**Finished:** 2026-08-02T00:00:27+00:00  

## Log
## 1. calibration-snapshotter under governance

- `23:59:42` ✅ schedule calibration-snapshotter-weekly already exists
- `23:59:42` ✅ under deploy management (LastModified 2026-08-01T23:57:17.000+0000)
## 2. party map -- canonical source, reachable host

- `00:00:16` invoked: {"statusCode": 200, "body": "{\"ok\": true, \"n_quiver\": 1000, \"n_house\": 500, \"n_senate\": 500, \"n_tickers\": 181, \"n_clusters\": 25, \"n_bipar
- `00:00:27` log: [political] live fetch failed -- keeping stale S3 map (better than the hardcoded floor)
- `00:00:27` log: [political] fetching Congress trades…
- `00:00:27` log: [political] HTTP 401 from https://api.quiverquant.com/beta/live/congresstrading
- `00:00:27` log: [political] live Quiver returned empty — trying S3 cache
- `00:00:27` log: [political] using S3 Quiver cache: 1000 trades (age 898.0h)
- `00:00:27` log: [political] got 1000 trades from: s3_cache_898.0h
- `00:00:27` log: [political] 181 unique tickers traded in last 90 days
- `00:00:27` log: [political] wrote 166,090B  duration=33.2s  tickers=181  clusters=25
## RESULT

- `00:00:27` ✗   party map still stale 89688 min
