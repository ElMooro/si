# ops 6586 - pipeline order, first light: did the retimed triggers fire at their new minutes?

**Status:** failure  
**Duration:** 8.3s  
**Finished:** 2026-10-22T23:59:54+00:00  

## Error

```
SystemExit: 1
```

## Data

| due | errors | fired_new | fired_old | lane | late_ticks_checked | late_ticks_ok |
|---|---|---|---|---|---|---|
| 173 | 3 | 172 | 2 | ok | 2 | 2 |

## Log
- `23:59:53` retimed triggers due so far today: 173; fired at the new minute: 172; still fired at the old minute: 2
- `23:59:53` ⚠ still fired at its OLD minute today: intraday-arm-daily cron(40 13 ? * MON-FRI *) (expected only when the apply came after that minute)
- `23:59:53` ⚠ still fired at its OLD minute today: justhodl-cusip-map-rebuild-hourly cron(28 1 * * ? *) (expected only when the apply came after that minute)
- `23:59:53` ⚠ no invocation in the 6 min from its new time: justhodl-stock-xray-daily cron(35 0 ? * MON-FRI *)
- `23:59:53` ⚠ errors after the retime: justhodl-dark-pool 3
- `23:59:53` ⚠ errors after the retime: justhodl-estimate-revisions 3
- `23:59:53` ⚠ errors after the retime: justhodl-research-backtest 2
- `23:59:54` late tick daily-morn-late: members invoked after cron(15 11 * * ? *): {"justhodl-khalid-metrics": 1.0, "justhodl-momentum-breakout": 1.0, "justhodl-morning-brief-tg": 1.0, "justhodl-nobrainer-rationale": 1.0, "justhodl-pre-pump-detector": 1.0, "justhodl-revenue-acceleration": 1.0}; scheduler log: ['[scheduler] tick=daily-morn-late ok=6 err=0 0.45s', '[scheduler] tick=daily-morn-late2 ok=3 err=0 0.32s']
- `23:59:54` late tick daily-morn-late2: members invoked after cron(30 11 * * ? *): {"justhodl-sector-earnings-diffusion": 1.0, "justhodl-theme-rotation-engine": 1.0, "justhodl-volatility-squeeze-hunter": 1.0}; scheduler log: ['[scheduler] tick=daily-morn-late2 ok=3 err=0 0.32s']
- `23:59:54` intraday lane: expected ~96 runs each since 09:00 NY; invocations {"justhodl-edge-engine": 96.0, "justhodl-implied-prob": 96.0, "justhodl-intraday-pulse": 97.0, "justhodl-market-map": 97.0, "justhodl-opex-calendar": 96.0, "justhodl-options-analytics": 97.0, "justhodl-options-gamma": 97.0, "justhodl-polygon-fx-regime": 97.0, "justhodl-risk-regime": 96.0, "justhodl-tail-risk": 96.0, "justhodl-trade-ticket-monitor": 97.0}; errors {} -> ok
- `23:59:54` ✗ timetable not confirmed: 172/173 fired at the new minute, 2 at the old, late ticks {'daily-morn-late': True, 'daily-morn-late2': True}, lane expected ~96 runs each since 09:00 NY; invocations {"justhodl-edge-engine": 96.0, "justhodl-implied-prob": 96.0, "justhodl-intraday-pulse": 97.0, "justhodl-market-map": 97.0, "justhodl-opex-calendar": 96.0, "justhodl-options-analytics": 97.0, "justhodl-options-gamma": 97.0, "justhodl-polygon-fx-regime": 97.0, "justhodl-risk-regime": 96.0, "justhodl-tail-risk": 96.0, "justhodl-trade-ticket-monitor": 97.0}; errors {}
