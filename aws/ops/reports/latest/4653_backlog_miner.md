# ops 4653 — backlog miner (SEC primary)

**Status:** success  
**Duration:** 35.8s  
**Finished:** 2026-08-13T20:59:15+00:00  

## Data

| budget_left | fn_error | mined | not_disclosed | targets |
|---|---|---|---|---|
|  | None |  |  |  |
| 0 |  | 20 | 23 | 79 |

## Log
## deploy (create-capable) + settle + schedule

- `20:58:41` ✅   [deploy] v1.1.0 live (created=False)
## run + mined truth

- `20:59:15` BA   MINED         lvl=694.7B         qoq=1.8    yoy=None   2026-04-22
- `20:59:15` CAT  MINED         lvl=51.2B          qoq=None   yoy=None   2026-02-13
- `20:59:15` LMT  MINED         lvl=0.2B           qoq=4.1    yoy=None   2026-04-23
- `20:59:15` GD   MINED         lvl=186.9B         qoq=-0.8   yoy=15.9   2026-07-29
- `20:59:15` DE   MINED         lvl=5.2B           qoq=None   yoy=None   2025-12-18
- `20:59:15` GE   NOT_DISCLOSED lvl=None           qoq=None   yoy=None   None
- `20:59:15` ✅   [mined] 20 mined, 20 plausible levels (coverage compounds via warm cache)
- `20:59:15` ✅   [sanity] no >300% deltas (guards): clean
- `20:59:15` ✅   [deltas] 12 tickers carry QoQ% (YoY fills as filings accumulate)
## verdict

- `20:59:15` ✅ BACKLOG MINER LIVE — 20/79 mined from SEC primary text · store data/backlog-mined.json
