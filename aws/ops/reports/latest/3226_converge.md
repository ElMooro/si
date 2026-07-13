# ops 3226 — FRED-first ordering; converge and count

**Status:** success  
**Duration:** 162.9s  
**Finished:** 2026-07-13T06:19:47+00:00  

## Data

| active_before | active_now | n_fails | n_warns | series_cached | verdict | woken |
|---|---|---|---|---|---|---|
| 117 | 118 |  |  | 2364 |  | 1 |
|  |  | 0 | 0 |  | PASS |  |

## Log
- `06:17:05`   zip: 79987 bytes
## 1. Lambda

- `06:17:05`   Lambda exists — updating
- `06:17:10` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `06:17:11`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `06:17:11` ✅   ✓ target → justhodl-wl-engines
- `06:17:11` ✅   ✓ added invoke permission
## Evidence: cache trajectory + trace

- `06:19:47`   [wl] cache=2364 new=50 114s
- `06:19:47`   [trace] TVC:ES10Y-TVC:IT10Y: weekly=0 zc=False
- `06:19:47`   [trace] ECONOMICS:GBDIR: weekly=0 zc=False
- `06:19:47`   [trace] TVC:FR10Y-TVC:IT10Y: weekly=422 zc=True
- `06:19:47`   [trace] ECONOMICS:EUDIR: weekly=1437 zc=True
- `06:19:47`   [trace] TVC:DE10Y-TVC:IT10Y: weekly=422 zc=True
- `06:19:47`   [wl] cache=2314 new=1 102s
- `06:19:47`   [trace] TVC:FR10Y-TVC:IT10Y: weekly=422 zc=True
- `06:19:47`   [trace] TVC:DE10Y-TVC:IT10Y: weekly=422 zc=True
- `06:19:47`   [trace] ECONOMICS:GBDIR: weekly=0 zc=False
- `06:19:47`   [trace] ECONOMICS:EUDIR: weekly=0 zc=False
- `06:19:47`   [trace] TVC:ES10Y-TVC:IT10Y: weekly=0 zc=False
## Wakes

- `06:19:47`   ⏰ WOKE: Forex
- `06:19:47` ✅ 1 panels WOKEN
