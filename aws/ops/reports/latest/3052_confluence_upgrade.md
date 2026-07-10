## 1. Wait both deploys, invoke both

**Status:** failure  
**Duration:** 74.1s  
**Finished:** 2026-07-10T13:32:56+00:00  

## Error

```
SystemExit: 1
```

## Data

| breadth_buying | breadth_top | confirmed_bottoms | confirmed_tops | cross_coverage | join_coverage | n_fails | n_warns | radar_deploy | radar_version | sample_confirm | sector_flows | top_sectors | verdict | whale_stance_n | whales_deploy | whales_schema |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | 2026-07-10T13:31:36.000+0000 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-07-10T13:32:15.000+0000 |  |
|  |  | 0 | 5 |  | {"dark_pool": 60, "whales": 400, "wyckoff": 400, "insiders": 2} |  |  |  | 1.3.0 | [{"t": "BE", "n": 2, "c": ["WHALES_13F", "WYCKOFF_PHASE"]}, {"t": "SMH", "n": 2, "c": ["WHALES_13F", "WYCKOFF_PHASE"]}, {"t": "ROKU", "n": 2, "c": ["DARK_POOL", "WHALES_13F"]}, {"t": "JBL", "n": 2, "c": ["WHALES_13F", "WYCKOFF_PHASE"]}] |  |  |  |  |  |  |
| 15 | [{"symbol": "FPS", "n_buying": 11, "n_selling": 0, "breadth": 11, "conviction_flow_usd": 293536362, "sector": null}, {"symbol": "VSNT", "n_buying": 10, "n_selling": 0, "breadth": 10, "conviction_flow_usd": 371879481, "sector": null}, {"symbol": "EQPT", "n_buying": 10, "n_selling": 0, "breadth": 10, "conviction_flow_usd": 168918306, "sector": null}] |  |  | {"dark_pool": 60, "wyckoff": 400, "radar": 70, "sectors": 0} |  |  |  |  |  |  | 0 | [] |  | 33 |  | 1.2 |
|  |  |  |  |  |  | 2 | 3 |  |  |  |  |  | FAIL |  |  |  |

## Log
## 2. Radar v1.3.0 asserts

## 3. Whales v1.2 asserts

## 4. Pages (warn-level, CDN)

## verdict

- `13:32:56` FAIL: whales join=400
- `13:32:56` FAIL: sector_flows=0 (<6)
