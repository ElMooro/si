# ops 4223 — premium param hunt

**Status:** success  
**Duration:** 11.4s  
**Finished:** 2026-08-01T03:46:15+00:00  

## Data

| catalog_n | feed_metrics | premium_won | ssr_now |
|---|---|---|---|
| 26 | 17 | False | 11.19456099 |

## Log
- `03:46:05`   miss btc/fund-data/coinbase-premium-index&exchange=coinbase: HTTP Error 400: Bad Request
- `03:46:05`   miss btc/fund-data/coinbase-premium-index&market=coinbase: HTTP Error 400: Bad Request
- `03:46:05`   miss btc/fund-data/coinbase-premium-index&exchange=coinbase_pro: HTTP Error 400: Bad Request
- `03:46:05`   miss btc/fund-data/coinbase-premium-gap&exchange=coinbase: HTTP Error 400: Bad Request
- `03:46:06`   miss btc/fund-data/market-premium&exchange=coinbase: HTTP Error 400: Bad Request
- `03:46:06`   miss btc/fund-data/korea-premium-index&exchange=upbit: HTTP Error 400: Bad Request
- `03:46:15` ✅ HUNT DONE — won=[] catalog=26
