# ops 4165 — convert the chewed queue

**Status:** success  
**Duration:** 511.4s  
**Finished:** 2026-07-31T21:22:36+00:00  

## Data

| family_labeled | honest_label_rows | pmi_labeled | total_live |
|---|---|---|---|
|  |  |  | 4798 |
| 123 |  | 89 |  |
|  | 761 |  |  |

## Log
- `21:14:15` ✅   justhodl-tradingview settled at loop 1
- `21:22:36` ✅   artifact after ~480s
- `21:22:36`   statuses: {"LIVE": 4798, "NO_FREE_SOURCE": 3799, "PENDING_RESOLUTION": 1459, "DISCONTINUED": 2, "META": 1}
- `21:22:36`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2787, "attempted: no free mirror (tv-proprietary/un": 761, "no free API for this indicator family (BIS/I": 123, "S&P Global PMI licensed": 89, "expired dated contract": 11, "continuation back-month: front-only mirrored": 7}
- `21:22:36`   LIVE by adapter: {"native": 1261, "fleet:finviz": 818, "feed:symbol": 437, "fmp": 351, "feed:cot": 260, "family:BOT": 144, "family:DIR": 136, "family:FI": 118, "family:GDG": 90, "family:M0": 75, "family:TOT": 60, "family:CBBS": 60}
- `21:22:36` ✅ CONVERTED — LIVE 4798
