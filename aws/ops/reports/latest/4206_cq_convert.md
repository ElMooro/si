# ops 4165 — convert the chewed queue

**Status:** success  
**Duration:** 352.9s  
**Finished:** 2026-07-31T23:33:21+00:00  

## Data

| family_labeled | honest_label_rows | pending_after | pmi_labeled | total_live |
|---|---|---|---|---|
|  |  |  |  | 5516 |
| 41 |  |  | 76 |  |
|  |  | 0 |  |  |
|  | 897 |  |  |  |

## Log
- `23:27:39` ✅   justhodl-tradingview settled at loop 1
- `23:33:21` ✅   artifact after ~330s
- `23:33:21`   statuses: {"LIVE": 5516, "NO_FREE_SOURCE": 4540, "DISCONTINUED": 2, "META": 1}
- `23:33:21`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2595, "country-indicator pair: no free agency sourc": 837, "attempted: no free mirror (tv-proprietary/un": 760, "exchange/venue has no free mirror": 137, "S&P Global PMI licensed": 76, "CFTC dataset lacks this column/market": 56}
- `23:33:21`   LIVE by adapter: {"native": 1261, "fleet:finviz": 818, "feed:te": 680, "feed:symbol": 437, "fmp": 351, "feed:cot": 260, "family:BOT": 144, "family:DIR": 136, "family:FI": 118, "family:GDG": 90, "family:M0": 75, "family:TOT": 60}
- `23:33:21` ✅ CONVERTED — LIVE 5516
