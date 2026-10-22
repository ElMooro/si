# ops 6586 - pipeline order, first light: did the retimed triggers fire at their new minutes?

**Status:** failure  
**Duration:** 0.9s  
**Finished:** 2026-10-22T03:38:35+00:00  

## Error

```
SystemExit: 1
```

## Data

| due | errors | fired_new | fired_old | lane | late_ticks_checked | late_ticks_ok |
|---|---|---|---|---|---|---|
| 3 | 0 | 2 | 1 | not as planned | 0 | 0 |

## Log
- `03:38:35` retimed triggers due so far today: 3; fired at the new minute: 2; still fired at the old minute: 1
- `03:38:35` ⚠ still fired at its OLD minute today: justhodl-cusip-map-rebuild-hourly cron(28 1 * * ? *) (expected only when the apply came after that minute)
- `03:38:35` ⚠ no invocation in the 6 min from its new time: justhodl-stock-xray-daily cron(35 0 ? * MON-FRI *)
- `03:38:35` late tick daily-morn-late not due yet today (cron(15 11 * * ? *) UTC)
- `03:38:35` late tick daily-morn-late2 not due yet today (cron(30 11 * * ? *) UTC)
- `03:38:35` intraday lane: expected ~96 runs each since 09:00 NY; invocations {"justhodl-edge-engine": 0, "justhodl-implied-prob": 0, "justhodl-intraday-pulse": 0, "justhodl-market-map": 0, "justhodl-opex-calendar": 0, "justhodl-options-analytics": 0, "justhodl-options-gamma": 0, "justhodl-polygon-futures-curves": 0, "justhodl-polygon-fx-regime": 0, "justhodl-risk-regime": 0, "justhodl-tail-risk": 0, "justhodl-trade-ticket-monitor": 0}; errors {} -> NOT AS PLANNED
- `03:38:35` ✗ timetable not confirmed: 2/3 fired at the new minute, 1 at the old, late ticks {}, lane expected ~96 runs each since 09:00 NY; invocations {"justhodl-edge-engine": 0, "justhodl-implied-prob": 0, "justhodl-intraday-pulse": 0, "justhodl-market-map": 0, "justhodl-opex-calendar": 0, "justhodl-options-analytics": 0, "justhodl-options-gamma": 0, "justhodl-polygon-futures-curves": 0, "justhodl-polygon-fx-regime": 0, "justhodl-risk-regime": 0, "justhodl-tail-risk": 0, "justhodl-trade-ticket-monitor": 0}; errors {}
