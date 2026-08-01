# ops 4222 — CQ round-2 probes + wires

**Status:** failure  
**Duration:** 116.4s  
**Finished:** 2026-08-01T03:42:13+00:00  

## Error

```
SystemExit: 1
```

## Data

| catalog_n | cqfeed | new_paths |
|---|---|---|
| 26 |  | 7 |
|  | {"metrics": 12} |  |

## Log
- `03:40:17`   NEW eth/exchange-flows/reserve: {"reserve": 15090836.17166374, "reserve_usd": 28973548980.656494}
- `03:40:17`   NEW eth/exchange-flows/netflow: {"netflow_total": -14326.927743188222}
- `03:40:19`   dead eth/market-indicator/mvrv: HTTP Error 404: Not Found
- `03:40:19`   dead eth/network-indicator/nupl: HTTP Error 404: Not Found
- `03:40:20`   NEW eth/network-data/supply: {"supply_total": 122328357.65610594, "supply_new": 3076.109363980552}
- `03:40:22`   dead eth/flow-indicator/exchange-whale-ratio: HTTP Error 404: Not Found
- `03:40:24`   NEW btc/market-data/funding-rates: {"funding_rates": 0.004867275}
- `03:40:24`   NEW btc/market-data/open-interest: {"open_interest": 22247127792.426075}
- `03:40:25`   NEW btc/market-data/liquidations: {"long_liquidations": 974.5271255, "short_liquidations": 190.52132117, "long_liquidations_
- `03:40:25`   NEW btc/market-data/taker-buy-sell-stats: {"taker_buy_volume": 6034831733.3628, "taker_sell_volume": 5986322622.8032, "taker_buy_rat
- `03:40:25`   dead btc/fund-data/coinbase-premium-index: HTTP Error 400: Bad Request
- `03:40:26`   dead btc/fund-data/coinbase-premium-gap: HTTP Error 400: Bad Request
- `03:40:26`   dead btc/inter-entity-flows/exchange-to-exchange: HTTP Error 400: Bad Request
- `03:41:43`   [cq_ssr] wired: {"marker": "ops4222", "ssr": null, "stablecoins_ratio_usd": 2.845727, "fund_flow_ratio": 0.05946532, "note": "dry-
- `03:41:57`   [cq_premium] wired: {"marker": "ops4222", "premium": null, "slug": null, "note": "CQ cross-check leg; null-honest until slug prove
- `03:42:13` ✅   bus + vault fired — alias thaws tonight
- `03:42:13` ✅   justhodl-altseason cq_ssr emitted
- `03:42:13` ✗   ssr plausible 3-40
- `03:42:13` ✅   justhodl-coinbase-premium cq_premium emitted
- `03:42:13` ✗ FAILED: ['ssr plausible 3-40']
