## 1. Whales v1.2 first (radar depends on it)

**Status:** success  
**Duration:** 74.5s  
**Finished:** 2026-07-10T13:39:26+00:00  

## Data

| breadth_buying | confirmed_bottoms | confirmed_tops | join_coverage | n_fails | n_warns | radar_version | sample | sector_flows | stocks_map | top_sectors | verdict | whale_stance_n | whales_schema |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 15 |  |  |  |  |  |  |  | 11 | 1500 | [{"s": "Technology", "net": -22976513863}, {"s": "Consumer Cyclical", "net": -12365542568}, {"s": "Energy", "net": -8428817448}, {"s": "Utilities", "net": -6356919274}, {"s": "Industrials", "net": 4460690300}] |  | 33 | 1.2 |
|  | 1 | 8 | {"dark_pool": 60, "whales": 1500, "wyckoff": 400, "insiders": 2} |  |  | 1.3.0 | [{"t": "COF", "n": 2, "c": ["DARK_POOL", "WHALES_13F"]}, {"t": "BE", "n": 2, "c": ["WHALES_13F", "WYCKOFF_PHASE"]}, {"t": "SMH", "n": 2, "c": ["WHALES_13F", "WYCKOFF_PHASE"]}, {"t": "ROKU", "n": 2, "c": ["DARK_POOL", "WHALES_13F"]}, {"t": "SMTC", "n": 2, "c": ["WHALES_13F", "WYCKOFF_PHASE"]}] |  |  |  |  |  |  |
|  |  |  |  | 0 | 0 |  |  |  |  |  | PASS |  |  |

## Log
## 2. Radar v1.3.0 (reads the fresh whale map)

## verdict

- `13:39:26` PASS
