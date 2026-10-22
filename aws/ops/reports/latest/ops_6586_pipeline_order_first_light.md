# ops 6586 - pipeline order, first light: did the retimed triggers fire at their new minutes?

**Status:** failure  
**Duration:** 2.3s  
**Finished:** 2026-10-22T03:32:18+00:00  

## Error

```
SystemExit: 1
```

## Data

| due | errors | fired_new | fired_old | lane | late_ticks_checked | late_ticks_ok |
|---|---|---|---|---|---|---|
| 1 | 0 | 0 | 0 | not as planned | 0 | 0 |

## Log
- `03:32:18` retimed triggers due so far today: 1; fired at the new minute: 0; still fired at the old minute: 0
- `03:32:18` ⚠ no invocation in the 6 min from its new time: justhodl-stock-xray-daily cron(35 0 ? * MON-FRI *)
- `03:32:18` late tick daily-morn-late not due yet today (cron(15 11 * * ? *) UTC)
- `03:32:18` late tick daily-morn-late2 not due yet today (cron(30 11 * * ? *) UTC)
- `03:32:18` intraday lane: expected ~96 runs each since 09:00 NY; invocations {"justhodl-edge-engine": 0, "justhodl-implied-prob": 0, "justhodl-intraday-pulse": 0, "justhodl-market-map": 0, "justhodl-opex-calendar": 0, "justhodl-options-analytics": 0, "justhodl-options-gamma": 0, "justhodl-polygon-futures-curves": 0, "justhodl-polygon-fx-regime": 0, "justhodl-risk-regime": 0, "justhodl-tail-risk": 0, "justhodl-trade-ticket-monitor": 0}; errors {} -> NOT AS PLANNED
- `03:32:18` ✗ timetable not confirmed: 0/1 fired at the new minute, 0 at the old, late ticks {}, lane expected ~96 runs each since 09:00 NY; invocations {"justhodl-edge-engine": 0, "justhodl-implied-prob": 0, "justhodl-intraday-pulse": 0, "justhodl-market-map": 0, "justhodl-opex-calendar": 0, "justhodl-options-analytics": 0, "justhodl-options-gamma": 0, "justhodl-polygon-futures-curves": 0, "justhodl-polygon-fx-regime": 0, "justhodl-risk-regime": 0, "justhodl-tail-risk": 0, "justhodl-trade-ticket-monitor": 0}; errors {}
