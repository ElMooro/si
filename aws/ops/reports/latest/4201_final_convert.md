# ops 4165 — convert the chewed queue

**Status:** success  
**Duration:** 313.4s  
**Finished:** 2026-07-31T22:26:12+00:00  

## Data

| family_labeled | honest_label_rows | pending_after | pmi_labeled | total_live |
|---|---|---|---|---|
|  |  |  |  | 5418 |
| 43 |  |  | 76 |  |
|  |  | 0 |  |  |
|  | 897 |  |  |  |

## Log
- `22:21:09` ✅   justhodl-tradingview settled at loop 1
- `22:26:12` ✅   artifact after ~285s
- `22:26:12`   statuses: {"LIVE": 5418, "NO_FREE_SOURCE": 4638, "DISCONTINUED": 2, "META": 1}
- `22:26:12`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2642, "country-indicator pair: no free agency sourc": 886, "attempted: no free mirror (tv-proprietary/un": 760, "exchange/venue has no free mirror": 137, "S&P Global PMI licensed": 76, "CFTC dataset lacks this column/market": 56}
- `22:26:12`   LIVE by adapter: {"native": 1261, "fleet:finviz": 818, "feed:te": 585, "feed:symbol": 437, "fmp": 351, "feed:cot": 260, "family:BOT": 144, "family:DIR": 136, "family:FI": 118, "family:GDG": 90, "family:M0": 75, "family:TOT": 60}
- `22:26:12` ✅ CONVERTED — LIVE 5418
