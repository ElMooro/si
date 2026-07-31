# ops 4165 — convert the chewed queue

**Status:** success  
**Duration:** 140.2s  
**Finished:** 2026-07-31T22:03:16+00:00  

## Data

| family_labeled | honest_label_rows | pending_after | pmi_labeled | total_live |
|---|---|---|---|---|
|  |  |  |  | 5436 |
| 52 |  |  | 76 |  |
|  |  | 0 |  |  |
|  | 898 |  |  |  |

## Log
- `22:01:07` ✅   justhodl-tradingview settled at loop 1
- `22:03:16` ✅   artifact after ~120s
- `22:03:16`   statuses: {"LIVE": 5436, "NO_FREE_SOURCE": 4620, "DISCONTINUED": 2, "META": 1}
- `22:03:16`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2604, "country-indicator pair: no free agency sourc": 897, "attempted: no free mirror (tv-proprietary/un": 761, "exchange/venue has no free mirror": 137, "S&P Global PMI licensed": 76, "CFTC dataset lacks this column/market": 56}
- `22:03:16`   LIVE by adapter: {"native": 1261, "fleet:finviz": 818, "feed:te": 603, "feed:symbol": 437, "fmp": 351, "feed:cot": 260, "family:BOT": 144, "family:DIR": 136, "family:FI": 118, "family:GDG": 90, "family:M0": 75, "family:TOT": 60}
- `22:03:16` ✅ CONVERTED — LIVE 5436
