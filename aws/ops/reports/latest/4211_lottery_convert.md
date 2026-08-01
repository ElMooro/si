# ops 4165 — convert the chewed queue

**Status:** success  
**Duration:** 118.6s  
**Finished:** 2026-08-01T00:13:10+00:00  

## Data

| family_labeled | honest_label_rows | lottery_pending | pending_after | pmi_labeled | total_live | yield_live |
|---|---|---|---|---|---|---|
|  |  |  |  |  | 5601 |  |
| 41 |  |  |  | 76 |  |  |
|  |  | 40 |  |  |  | 0 |
|  |  |  | 40 |  |  |  |
|  | 897 |  |  |  |  |  |

## Log
- `00:11:22` ✅   justhodl-tradingview settled at loop 1
- `00:13:10` ✅   artifact after ~105s
- `00:13:10`   statuses: {"LIVE": 5601, "NO_FREE_SOURCE": 4415, "PENDING_RESOLUTION": 40, "DISCONTINUED": 2, "META": 1}
- `00:13:10`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2474, "country-indicator pair: no free agency sourc": 837, "attempted: no free mirror (tv-proprietary/un": 760, "exchange/venue has no free mirror": 137, "S&P Global PMI licensed": 76, "CFTC dataset lacks this column/market": 56}
- `00:13:10`   LIVE by adapter: {"native": 1261, "fleet:finviz": 818, "feed:te": 764, "feed:symbol": 438, "fmp": 351, "feed:cot": 260, "family:BOT": 144, "family:DIR": 136, "family:FI": 118, "family:GDG": 90, "family:M0": 75, "family:TOT": 60}
- `00:13:10` ✅ CONVERTED — LIVE 5601
