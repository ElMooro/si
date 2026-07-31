# ops 4205 — CryptoQuant rail

**Status:** failure  
**Duration:** 268.8s  
**Finished:** 2026-07-31T23:27:28+00:00  

## Error

```
SystemExit: 1
```

## Data

| cq1 | cq1_err | cq2 | cq2_err | dead | routed | targets |
|---|---|---|---|---|---|---|
| {"routed": 1, "targets": 212} | None |  |  |  |  |  |
|  |  | {"routed": 1, "targets": 212} | None |  |  |  |
|  |  |  |  | 118 | 1 | 212 |

## Log
- `23:23:10` ✅   justhodl-cq-feed settled at loop 1
- `23:27:17`   route BTC_HASHRATE: {"value": 960075344981.735, "asof": "2026-07-30", "src": "btc/network-data/hashrate.hashrate"}
- `23:27:27` ✅   justhodl-tradingview settled at loop 1
- `23:27:28` ✅   schedule cq-feed-daily
- `23:27:28` ✅   cq-feed settled
- `23:27:28` ✗   cq routed >= 15
- `23:27:28` ✅   vault v3.29.0 settled
- `23:27:28` ✗ FAILED: ['cq routed >= 15']
