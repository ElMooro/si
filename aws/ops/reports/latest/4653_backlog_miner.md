# ops 4653 — backlog miner (SEC primary)

**Status:** success  
**Duration:** 41.2s  
**Finished:** 2026-08-13T21:04:45+00:00  

## Data

| budget_left | fn_error | mined | not_disclosed | targets |
|---|---|---|---|---|
|  | None |  |  |  |
| 0 |  | 22 | 47 | 133 |

## Log
## deploy (create-capable) + settle + schedule

- `21:04:05` ✅   [deploy] v1.1.0 live (created=False)
## run + mined truth

- `21:04:45` BA   MINED         lvl=694.7B         qoq=1.8    yoy=None   2026-04-22
- `21:04:45` CAT  MINED         lvl=51.2B          qoq=None   yoy=None   2026-02-13
- `21:04:45` LMT  MINED         lvl=0.2B           qoq=4.1    yoy=None   2026-04-23
- `21:04:45` GD   MINED         lvl=186.9B         qoq=-0.8   yoy=15.9   2026-07-29
- `21:04:45` DE   MINED         lvl=5.2B           qoq=None   yoy=None   2025-12-18
- `21:04:45` GE   NOT_DISCLOSED lvl=None           qoq=None   yoy=None   None
- `21:04:45` ✅   [mined] 22 mined, 22 plausible levels (coverage compounds via warm cache)
- `21:04:45` ✅   [sanity] no >300% deltas (guards): clean
- `21:04:45` ✅   [deltas] 13 tickers carry QoQ% (YoY fills as filings accumulate)
## verdict

- `21:04:45` ✅ BACKLOG MINER LIVE — 22/133 mined from SEC primary text · store data/backlog-mined.json
