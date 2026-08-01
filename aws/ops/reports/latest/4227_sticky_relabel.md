# ops 4226 — onchain truth

**Status:** failure  
**Duration:** 103.9s  
**Finished:** 2026-08-01T05:13:48+00:00  

## Error

```
SystemExit: 1
```

## Data

| cq_live | live | nfs | provider_labeled |
|---|---|---|---|
| 2 | 5605 | 4414 | 198 |

## Log
- `05:12:06`   BTC_SOPR: {"status": "LIVE", "src": "cryptoquant:btc/market-indic", "note": "no free API found (TV/TradingEconomics"}
- `05:12:06`   GLASSNODE:BTC_SOPR: ABSENT
- `05:12:06`   BTC_HASHRATE: {"status": "LIVE", "src": "cryptoquant:btc/network-data", "note": "no free API found (TV/TradingEconomics"}
- `05:12:06`   USDT_SUPPLY: {"status": "NO_FREE_SOURCE", "src": "unresolved_tv_only", "note": "no free API found (TV/TradingEconomics"}
- `05:12:06`   ERC20_WHALES: {"status": "NO_FREE_SOURCE", "src": "unresolved_tv_only", "note": "no free API found (TV/TradingEconomics"}
- `05:12:06` ✗   justhodl-tradingview: marker missing in checkout
- `05:13:48` ✗   vault v3.30.1 settled
- `05:13:48` ✅   fresh artifact
- `05:13:48` ✅   provider labels >= 120
- `05:13:48` ✗ FAILED: ['vault v3.30.1 settled']
