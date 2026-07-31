# ops 4165 — convert the chewed queue

**Status:** success  
**Duration:** 502.0s  
**Finished:** 2026-07-31T19:23:46+00:00  

## Data

| honest_label_rows | total_live |
|---|---|
|  | 4604 |
| 761 |  |

## Log
- `19:15:34` ✅   justhodl-tradingview settled at loop 1
- `19:23:46` ✅   artifact after ~465s
- `19:23:46`   statuses: {"LIVE": 4604, "NO_FREE_SOURCE": 3442, "PENDING_RESOLUTION": 2010, "DISCONTINUED": 2, "META": 1}
- `19:23:46`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2662, "attempted: no free mirror (tv-proprietary/un": 761, "S&P Global PMI licensed": 8, "3M TB not in MOF JGB CSV \u2014 BOJ API next": 1, "referenced in eurodollar-plumbing code, not ": 1, "FTSE licensed": 1}
- `19:23:46`   LIVE by adapter: {"native": 1261, "fleet:finviz": 818, "feed:symbol": 437, "fmp": 351, "feed:cot": 240, "family:BOT": 144, "family:DIR": 136, "family:FI": 118, "family:GDG": 90, "family:M0": 75, "family:TOT": 60, "family:CBBS": 60}
- `19:23:46` ✅ CONVERTED — LIVE 4604
