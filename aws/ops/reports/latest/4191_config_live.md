# ops 4165 — convert the chewed queue

**Status:** success  
**Duration:** 504.7s  
**Finished:** 2026-07-31T20:17:34+00:00  

## Data

| honest_label_rows | total_live |
|---|---|
|  | 4731 |
| 761 |  |

## Log
- `20:09:20` ✅   justhodl-tradingview settled at loop 1
- `20:17:34` ✅   artifact after ~465s
- `20:17:34`   statuses: {"LIVE": 4731, "NO_FREE_SOURCE": 3463, "PENDING_RESOLUTION": 1862, "DISCONTINUED": 2, "META": 1}
- `20:17:34`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2676, "attempted: no free mirror (tv-proprietary/un": 761, "S&P Global PMI licensed": 8, "continuation back-month: front-only mirrored": 7, "3M TB not in MOF JGB CSV \u2014 BOJ API next": 1, "referenced in eurodollar-plumbing code, not ": 1}
- `20:17:34`   LIVE by adapter: {"native": 1261, "fleet:finviz": 818, "feed:symbol": 437, "fmp": 351, "feed:cot": 240, "family:BOT": 144, "family:DIR": 136, "family:FI": 118, "family:GDG": 90, "family:M0": 75, "family:TOT": 60, "family:CBBS": 60}
- `20:17:34` ✅ CONVERTED — LIVE 4731
