## 1. S3 object

**Status:** success  
**Duration:** 1.0s  
**Finished:** 2026-07-10T13:01:23+00:00  

## Data

| age_h | as_of | bucket_cors | cron | fn_modified | rel_route | rule | s3_exists | s3_route | size | state | status | top_keys |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 15.0 |  |  |  |  |  |  | True |  | 2812 |  |  |  |
|  | 2026-07-09T22:00:42.935887+00:00 |  |  |  |  |  |  |  |  | NULL |  | ["as_of", "cooldown_until", "current_readings", "engine", "forward_expectations", "historical_episodes", "methodology", "prev_state", "recommended_trade", "schedule", "signal_strength", "sources", "state", "state_since"] |
|  |  |  |  |  | {"code": 200, "len": 2812, "cors": "*", "head": "{\"engine\": \"breadth-thrust\", \"version\": \"1.0\", \"as_of\": \"2026-07-09T22:00:42.935887+00:00\", \"state\": \"NULL\", \"prev_state"} |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | {"code": 200, "len": 2812, "cors": "*", "head": "{\"engine\": \"breadth-thrust\", \"version\": \"1.0\", \"as_of\": \"2026-07-09T22:00:42.935887+00:00\", \"state\": \"NULL\", \"prev_state"} |  |  |  |  |
|  |  | [{"AllowedHeaders": ["*"], "AllowedMethods": ["GET", "HEAD"], "AllowedOrigins": ["https://justhodl.ai", "https://elmooro.github.io", "http://localhost:*", "*"], "ExposeHeaders": ["ETag"], "MaxAgeSeconds": 300}] |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | 2026-06-23T18:00:10.000+0000 |  |  |  |  |  | Active | Successful |  |
|  |  |  | cron(0 22 ? * MON-FRI *) |  |  | ENABLED |  |  |  |  |  |  |

## Log
## 2. Routes (runner-side)

## 3. Engine + schedule

- `13:01:23` evidence-only pass done
