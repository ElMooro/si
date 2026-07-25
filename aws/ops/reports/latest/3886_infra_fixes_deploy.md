# ops 3886 — deploy news-wire/news-sentiment/feed-catalog fixes, hard-gate on real evidence

**Status:** failure  
**Duration:** 948.9s  
**Finished:** 2026-07-25T21:53:04+00:00  

## Error

```
SystemExit: 1
```

## Data

| earnings_tracker_schema_type | earnings_tracker_writers | n_etf_flows_feeds_now_catalogued | rebalance_radar_schema_type | rebalance_radar_writers | total_feeds |
|---|---|---|---|---|---|
| object | ['justhodl-earnings-tracker'] | 418 | object | ['justhodl-rebalance-radar'] | 28069 |

## Log
## 1. news-wire — config-only change (inherit_env), verify the LIVE env var directly

- `21:37:15`   justhodl-news-wire: attempt 1, ANTHROPIC_API_KEY=still placeholder/short
- `21:37:30`   justhodl-news-wire: attempt 2, ANTHROPIC_API_KEY=still placeholder/short
- `21:37:45`   justhodl-news-wire: attempt 3, ANTHROPIC_API_KEY=still placeholder/short
- `21:38:00`   justhodl-news-wire: attempt 4, ANTHROPIC_API_KEY=still placeholder/short
- `21:38:16`   justhodl-news-wire: attempt 5, ANTHROPIC_API_KEY=still placeholder/short
- `21:38:31`   justhodl-news-wire: attempt 6, ANTHROPIC_API_KEY=still placeholder/short
- `21:38:46`   justhodl-news-wire: attempt 7, ANTHROPIC_API_KEY=still placeholder/short
- `21:39:01`   justhodl-news-wire: attempt 8, ANTHROPIC_API_KEY=still placeholder/short
- `21:39:16`   justhodl-news-wire: attempt 9, ANTHROPIC_API_KEY=still placeholder/short
- `21:39:31`   justhodl-news-wire: attempt 10, ANTHROPIC_API_KEY=still placeholder/short
- `21:39:46`   justhodl-news-wire: attempt 11, ANTHROPIC_API_KEY=still placeholder/short
- `21:40:01`   justhodl-news-wire: attempt 12, ANTHROPIC_API_KEY=still placeholder/short
- `21:40:16`   justhodl-news-wire: attempt 13, ANTHROPIC_API_KEY=still placeholder/short
- `21:40:31`   justhodl-news-wire: attempt 14, ANTHROPIC_API_KEY=still placeholder/short
- `21:40:47`   justhodl-news-wire: attempt 15, ANTHROPIC_API_KEY=still placeholder/short
- `21:41:02`   justhodl-news-wire: attempt 16, ANTHROPIC_API_KEY=still placeholder/short
- `21:41:17`   justhodl-news-wire: attempt 17, ANTHROPIC_API_KEY=still placeholder/short
- `21:41:32`   justhodl-news-wire: attempt 18, ANTHROPIC_API_KEY=still placeholder/short
- `21:41:47`   justhodl-news-wire: attempt 19, ANTHROPIC_API_KEY=still placeholder/short
- `21:42:02`   justhodl-news-wire: attempt 20, ANTHROPIC_API_KEY=still placeholder/short
- `21:42:17`   justhodl-news-wire: attempt 21, ANTHROPIC_API_KEY=still placeholder/short
- `21:42:32`   justhodl-news-wire: attempt 22, ANTHROPIC_API_KEY=still placeholder/short
- `21:42:48`   justhodl-news-wire: attempt 23, ANTHROPIC_API_KEY=still placeholder/short
- `21:43:03`   justhodl-news-wire: attempt 24, ANTHROPIC_API_KEY=still placeholder/short
- `21:43:18`   justhodl-news-wire: attempt 25, ANTHROPIC_API_KEY=still placeholder/short
- `21:43:33`   justhodl-news-wire: attempt 26, ANTHROPIC_API_KEY=still placeholder/short
- `21:43:48`   justhodl-news-wire: attempt 27, ANTHROPIC_API_KEY=still placeholder/short
- `21:44:03`   justhodl-news-wire: attempt 28, ANTHROPIC_API_KEY=still placeholder/short
- `21:44:18`   justhodl-news-wire: attempt 29, ANTHROPIC_API_KEY=still placeholder/short
- `21:44:33`   justhodl-news-wire: attempt 30, ANTHROPIC_API_KEY=still placeholder/short
- `21:44:48` ✗   justhodl-news-wire: ANTHROPIC_API_KEY never became a real live value after 30 attempts
## 2. news-sentiment — zip-settle (source fix) + env-key (config fix) + invoke + gate

- `21:44:49` ✅   justhodl-news-sentiment: new artifact live on attempt 1
- `21:44:49`   justhodl-news-sentiment: attempt 1, ANTHROPIC_API_KEY=empty
- `21:45:04`   justhodl-news-sentiment: attempt 2, ANTHROPIC_API_KEY=empty
- `21:45:19`   justhodl-news-sentiment: attempt 3, ANTHROPIC_API_KEY=empty
- `21:45:34`   justhodl-news-sentiment: attempt 4, ANTHROPIC_API_KEY=empty
- `21:45:49`   justhodl-news-sentiment: attempt 5, ANTHROPIC_API_KEY=empty
- `21:46:04`   justhodl-news-sentiment: attempt 6, ANTHROPIC_API_KEY=empty
- `21:46:19`   justhodl-news-sentiment: attempt 7, ANTHROPIC_API_KEY=empty
- `21:46:35`   justhodl-news-sentiment: attempt 8, ANTHROPIC_API_KEY=empty
- `21:46:50`   justhodl-news-sentiment: attempt 9, ANTHROPIC_API_KEY=empty
- `21:47:05`   justhodl-news-sentiment: attempt 10, ANTHROPIC_API_KEY=empty
- `21:47:20`   justhodl-news-sentiment: attempt 11, ANTHROPIC_API_KEY=empty
- `21:47:35`   justhodl-news-sentiment: attempt 12, ANTHROPIC_API_KEY=empty
- `21:47:50`   justhodl-news-sentiment: attempt 13, ANTHROPIC_API_KEY=empty
- `21:48:05`   justhodl-news-sentiment: attempt 14, ANTHROPIC_API_KEY=empty
- `21:48:20`   justhodl-news-sentiment: attempt 15, ANTHROPIC_API_KEY=empty
- `21:48:35`   justhodl-news-sentiment: attempt 16, ANTHROPIC_API_KEY=empty
- `21:48:51`   justhodl-news-sentiment: attempt 17, ANTHROPIC_API_KEY=empty
- `21:49:06`   justhodl-news-sentiment: attempt 18, ANTHROPIC_API_KEY=empty
- `21:49:21`   justhodl-news-sentiment: attempt 19, ANTHROPIC_API_KEY=empty
- `21:49:36`   justhodl-news-sentiment: attempt 20, ANTHROPIC_API_KEY=empty
- `21:49:51`   justhodl-news-sentiment: attempt 21, ANTHROPIC_API_KEY=empty
- `21:50:06`   justhodl-news-sentiment: attempt 22, ANTHROPIC_API_KEY=empty
- `21:50:21`   justhodl-news-sentiment: attempt 23, ANTHROPIC_API_KEY=empty
- `21:50:36`   justhodl-news-sentiment: attempt 24, ANTHROPIC_API_KEY=empty
- `21:50:51`   justhodl-news-sentiment: attempt 25, ANTHROPIC_API_KEY=empty
- `21:51:06`   justhodl-news-sentiment: attempt 26, ANTHROPIC_API_KEY=empty
- `21:51:22`   justhodl-news-sentiment: attempt 27, ANTHROPIC_API_KEY=empty
- `21:51:37`   justhodl-news-sentiment: attempt 28, ANTHROPIC_API_KEY=empty
- `21:51:52`   justhodl-news-sentiment: attempt 29, ANTHROPIC_API_KEY=empty
- `21:52:07`   justhodl-news-sentiment: attempt 30, ANTHROPIC_API_KEY=empty
- `21:52:22` ✗   justhodl-news-sentiment: ANTHROPIC_API_KEY never became a real live value after 30 attempts
## 3. feed-catalog — zip-settle + invoke + gate on real schema+writers for the 2 target engines

- `21:52:23` ✅   justhodl-feed-catalog: new artifact live on attempt 1
- `21:52:23` ✅   justhodl-feed-catalog: State=Active LastUpdateStatus=Successful Memory=2048 Timeout=840
- `21:53:03` ✅   feed-catalog.json rewritten on attempt 2
- `21:53:04`   recent log tail: 7.943Z	8f10f676-9f28-4dd8-ae05-83fe17ac5f8e	Connection pool is full, discarding connection: justhodl-dashboard-live.s3.amazonaws.com. Connection pool size: 10

[WARNING]	2026-07-25T21:52:47.944Z	8f10f676-9f28-4dd8-ae05-83fe17ac5f8e	Connection pool is full, discarding connection: justhodl-dashboard-live.s3.amazonaws.com. Connection pool size: 10

[WARNING]	2026-07-25T21:52:47.950Z	8f10f676-9f28-4dd8-ae05-83fe17ac5f8e	Connection pool is full, discarding connection: justhodl-dashboard-live.s3.amazonaws.com. Connection pool size: 10

[WARNING]	2026-07-25T21:52:47.978Z	8f10f676-9f28-4dd8-ae05-83fe17ac5f8e	Connection pool is full, discarding connection: justhodl-dashboard-live.s3.amazonaws.com. Connection pool size: 10

[WARNING]	2026-07-25T21:52:47.993Z	8f10f676-9f28-4dd8-ae05-83fe17ac5f8e	Connection pool is full, discarding connection: justhodl-dashboard-live.s3.amazonaws.com. Connection pool size: 10

[feed-catalog] sampled 4000/28069 feeds (cap=4000, by-recency)

[feed-catalog] writers: 761 keys from engine-manifest, 104 keys from description-scan, 769 keys total

[feed-catalog] OK — 28069 feeds, 31.06s

[jhcore.notify] telegram err: HTTP Error 401: Unauthorized

END RequestId: 8f10f676-9f28-4dd8-ae05-83fe17ac5f8e

REPORT RequestId: 8f10f676-9f28-4dd8-ae05-83fe17ac5f8e	Duration: 31414.74 ms	Billed Duration: 31964 ms	Memory Size: 2048 MB	Max Memory Used: 273 MB	Init Duration: 548.72 ms	
XRAY TraceId: 1-6a653017-20b75f614dfd56bc355dd82c	SegmentId: db31c2947a6182b9	Sampled: true	

## 4. THE HARD GATE — every claim checked against real deployed evidence

- `21:53:04` ✗   news-wire: no 401/Unauthorized in the invoke that followed the fix
- `21:53:04` ✗   news-sentiment: produced fresh output
- `21:53:04` ✅   rebalance-radar.json now has a real inferred schema (was 'not sampled')
- `21:53:04` ✅   rebalance-radar.json writer attribution now correct
- `21:53:04` ✅   earnings-tracker.json now has a real inferred schema (was 'not sampled')
- `21:53:04` ✅   earnings-tracker.json writer attribution now correct
- `21:53:04` ✅   etf-flows/* feeds now catalogued at all (was structurally excluded)
- `21:53:04` ✗ FAILED 2: ['news-wire: no 401/Unauthorized in the invoke that followed the fix', 'news-sentiment: produced fresh output']
