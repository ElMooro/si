# ops 4165 — convert the chewed queue

**Status:** success  
**Duration:** 510.7s  
**Finished:** 2026-07-31T21:49:04+00:00  

## Data

| family_labeled | honest_label_rows | pending_after | pmi_labeled | total_live |
|---|---|---|---|---|
|  |  |  |  | 4833 |
| 123 |  |  | 89 |  |
|  |  | 31 |  |  |
|  | 898 |  |  |  |

## Log
- `21:40:52` ✅   justhodl-tradingview settled at loop 1
- `21:49:04` ✅   artifact after ~465s
- `21:49:04`   statuses: {"NO_FREE_SOURCE": 5192, "LIVE": 4833, "PENDING_RESOLUTION": 31, "DISCONTINUED": 2, "META": 1}
- `21:49:04`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2882, "country-indicator pair: no free agency sourc": 1105, "attempted: no free mirror (tv-proprietary/un": 761, "exchange/venue has no free mirror": 137, "no free API for this indicator family (BIS/I": 123, "S&P Global PMI licensed": 89}
- `21:49:04`   LIVE by adapter: {"native": 1261, "fleet:finviz": 818, "feed:symbol": 437, "fmp": 351, "feed:cot": 260, "family:BOT": 144, "family:DIR": 136, "family:FI": 118, "family:GDG": 90, "family:M0": 75, "family:TOT": 60, "family:CBBS": 60}
- `21:49:04` ✅ CONVERTED — LIVE 4833
