# ops 4226 — onchain truth

**Status:** failure  
**Duration:** 386.9s  
**Finished:** 2026-08-01T05:23:39+00:00  

## Error

```
SystemExit: 1
```

## Data

| cq_live | live | nfs | provider_labeled |
|---|---|---|---|
| 2 | 5521 | 4498 | 198 |

## Log
- `05:17:13`   BTC_SOPR: {"status": "LIVE", "src": "cryptoquant:btc/market-indic", "note": "no free API found (TV/TradingEconomics"}
- `05:17:13`   GLASSNODE:BTC_SOPR: ABSENT
- `05:17:13`   BTC_HASHRATE: {"status": "LIVE", "src": "cryptoquant:btc/network-data", "note": "no free API found (TV/TradingEconomics"}
- `05:17:13`   USDT_SUPPLY: {"status": "NO_FREE_SOURCE", "src": "unresolved_tv_only", "note": "no free API found (TV/TradingEconomics"}
- `05:17:13`   ERC20_WHALES: {"status": "NO_FREE_SOURCE", "src": "unresolved_tv_only", "note": "on-chain provider-licensed (Glassnode/"}
- `05:17:13` ✗   justhodl-tradingview: marker missing in checkout
- `05:23:39` ✗   vault v3.30.1 settled
- `05:23:39` ✅   fresh artifact
- `05:23:39` ✅   provider labels >= 120
- `05:23:39` ✗ FAILED: ['vault v3.30.1 settled']
