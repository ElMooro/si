# ops 6656 - fmp-walker price lane: its own daily run (22:40 UTC), declaration, first run at 18 req/s

**Status:** success  
**Duration:** 735.5s  
**Finished:** 2026-10-26T01:50:30+00:00  

## Data

| calls | entities | http429 | ok | step | universe |
|---|---|---|---|---|---|
| 3024 | 756 | 756 | None | price lane dedicated run | 17914 |

## Log
## 1. the deployed function carries v1.2.1 (18 req/s)

- `01:38:15` justhodl-fmp-walker modified 2026-10-26T00:57:15, timeout 900, memory 2048, FMP_WALKER_RPS 18 WORKERS 12
## 2. schedule

- `01:38:16` ✅ created justhodl-fmp-walker-prices: cron(40 22 * * ? *) ENABLED target justhodl-fmp-walker role justhodl-scheduler-role
- `01:38:17` ✅ manifest declared ['justhodl-fmp-walker-prices'] (backup backups/schedule-manifest/20261026T013816Z/)
## 3. first price-lane run now (lease-guarded, async; waits for the state write)

- `01:38:18` async invoke 202 at 01:38:18; lane cursor before: {"cursor": 12950, "lap": 1, "entities_last_run": 384, "universe_size": 17914}
- `01:50:30` run: calls 3024 puts 0 seconds 724.3 http {"http429": 756} steps [{"step": "lists", "seconds": 0.0, "calls": 0}, {"step": "latest", "seconds": 0.0, "calls": 0}, {"step": "dated+window", "seconds": 0.0, "calls": 0}, {"step": "per_entity_daily", "seconds": 713.5, "calls": 3024, "datasets": ["historical-price-eod-full-daily"]}, {"step": "per_entity", "seconds": 0.0, "calls": 3024}]
- `01:50:30` lane after: cursor 13706 lap 1 entities_last_run 756 universe_size 17914
- `01:50:30` ⚠ 429s still high at 18 req/s - the key's limit is lower than the fleet assumed; lower FMP_WALKER_RPS further
- `01:50:30` ✅ ops 6656 complete
