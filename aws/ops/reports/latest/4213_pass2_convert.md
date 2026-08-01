# ops 4165 — convert the chewed queue

**Status:** success  
**Duration:** 398.5s  
**Finished:** 2026-08-01T01:23:00+00:00  

## Data

| family_labeled | honest_label_rows | lottery_pending | pending_after | pmi_labeled | total_live | yield_live |
|---|---|---|---|---|---|---|
|  |  |  |  |  | 5517 |  |
| 41 |  |  |  | 76 |  |  |
|  |  | 40 |  |  |  | 0 |
|  |  |  | 40 |  |  |  |
|  | 897 |  |  |  |  |  |

## Log
- `01:16:32` ✅   justhodl-tradingview settled at loop 1
- `01:23:00` ✅   artifact after ~360s
- `01:23:00`   statuses: {"LIVE": 5517, "NO_FREE_SOURCE": 4499, "PENDING_RESOLUTION": 40, "DISCONTINUED": 2, "META": 1}
- `01:23:00`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2554, "country-indicator pair: no free agency sourc": 837, "attempted: no free mirror (tv-proprietary/un": 760, "exchange/venue has no free mirror": 137, "S&P Global PMI licensed": 76, "CFTC dataset lacks this column/market": 56}
- `01:23:00`   LIVE by adapter: {"native": 1261, "fleet:finviz": 818, "feed:te": 680, "feed:symbol": 438, "fmp": 351, "feed:cot": 260, "family:BOT": 144, "family:DIR": 136, "family:FI": 118, "family:GDG": 90, "family:M0": 75, "family:TOT": 60}
- `01:23:00` ✅ CONVERTED — LIVE 5517
