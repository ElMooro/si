## 1. Warroom v12 regen

**Status:** failure  
**Duration:** 91.5s  
**Finished:** 2026-07-10T02:50:32+00:00  

## Error

```
SystemExit: 1
```

## Data

| ddb_rows | drill | earned | feed_ages_h | harvester | health | lane | n_fails | n_warns | stale | top_picks | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | 39.5 | {"macro_grid": 2.3, "funding": 15.2, "leading_markets": 4.3, "dollar": 1.6, "vol": 2.8, "ciss": 19.6, "factor_regime": 5.4, "cftc": 13.8, "global_stress": 1.2, "plumbing": 4.3, "eurodollar": 14.8, "alerts": 12.1} |  |  |  |  |  | [] | [{"ticker": "SPY", "direction": "LONG", "score": 0.5, "note": "barometer risk-on band"}] |  |
|  |  |  |  | {"statusCode": 200, "body": "{\"ok\": true, \"engines\": 141, \"written\": 515, \"scanned\": 560}"} |  |  |  |  |  |  |  |
| 0 |  |  |  |  |  |  |  |  |  |  |  |
|  | False |  |  |  | False | False |  |  |  |  |  |
|  |  |  |  |  |  |  | 1 | 1 |  |  | FAIL |

## Log
## 2. Harvester ingest

- `02:50:32` scorecard grading matures in ~3 weeks; alpha verdict will appear in engine-alpha.json automatically.
## 3. Live page (warn-level)

## verdict

- `02:50:32` FAIL: no eng:canary-warroom row after harvest
