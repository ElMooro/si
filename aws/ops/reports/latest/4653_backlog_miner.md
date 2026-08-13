# ops 4653 — backlog miner (SEC primary)

**Status:** failure  
**Duration:** 3.3s  
**Finished:** 2026-08-13T20:09:52+00:00  

## Error

```
SystemExit: 1
```

## Data

| budget_left | fn_error | mined | not_disclosed | targets |
|---|---|---|---|---|
|  | None |  |  |  |
| 22 |  | 18 | 7 | 25 |

## Log
## deploy (create-capable) + settle + schedule

- `20:09:49` ✅   [deploy] v1.0.1 live (created=False)
## run + mined truth

- `20:09:52` BA   MINED         lvl=694.7B         qoq=1.8    yoy=None   2026-04-22
- `20:09:52` CAT  MINED         lvl=51.2B          qoq=None   yoy=None   2026-02-13
- `20:09:52` LMT  MINED         lvl=36.8B          qoq=19642.5 yoy=22002.1 2026-07-23
- `20:09:52` GD   MINED         lvl=186.9B         qoq=-0.8   yoy=15.9   2026-07-29
- `20:09:52` DE   MINED         lvl=5.2B           qoq=None   yoy=None   2025-12-18
- `20:09:52` GE   NOT_DISCLOSED lvl=None           qoq=None   yoy=None   None
- `20:09:52` ✅   [mined] 18 mined, 18 plausible levels (coverage compounds via warm cache)
- `20:09:52` ✗   [sanity] CONTRACT MISS — no >300% deltas (guards): ['LMT', 'ETN', 'FLR']
- `20:09:52` ✅   [deltas] 13 tickers carry QoQ% (YoY fills as filings accumulate)
## verdict

- `20:09:52` ✗ backlog-miner: 1 red
