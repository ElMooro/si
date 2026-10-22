# ops 6586 - pipeline order, first light: did the retimed triggers fire at their new minutes?

**Status:** failure  
**Duration:** 5.4s  
**Finished:** 2026-10-22T13:30:15+00:00  

## Error

```
SystemExit: 1
```

## Data

| due | errors | fired_new | fired_old | lane | late_ticks_checked | late_ticks_ok |
|---|---|---|---|---|---|---|
| 41 | 2 | 40 | 1 | n/a | 2 | 2 |

## Log
- `13:30:14` retimed triggers due so far today: 41; fired at the new minute: 40; still fired at the old minute: 1
- `13:30:14` ⚠ still fired at its OLD minute today: justhodl-cusip-map-rebuild-hourly cron(28 1 * * ? *) (expected only when the apply came after that minute)
- `13:30:14` ⚠ no invocation in the 6 min from its new time: justhodl-stock-xray-daily cron(35 0 ? * MON-FRI *)
- `13:30:14` ⚠ errors after the retime: justhodl-estimate-revisions 3
- `13:30:14` ⚠ errors after the retime: justhodl-research-backtest 2
- `13:30:15` late tick daily-morn-late: members invoked after cron(15 11 * * ? *): {"justhodl-khalid-metrics": 1.0, "justhodl-momentum-breakout": 1.0, "justhodl-morning-brief-tg": 1.0, "justhodl-nobrainer-rationale": 1.0, "justhodl-pre-pump-detector": 1.0, "justhodl-revenue-acceleration": 1.0}; scheduler log: ['[scheduler] tick=daily-morn-late ok=6 err=0 0.45s', '[scheduler] tick=daily-morn-late2 ok=3 err=0 0.32s']
- `13:30:15` late tick daily-morn-late2: members invoked after cron(30 11 * * ? *): {"justhodl-sector-earnings-diffusion": 1.0, "justhodl-theme-rotation-engine": 1.0, "justhodl-volatility-squeeze-hunter": 1.0}; scheduler log: ['[scheduler] tick=daily-morn-late2 ok=3 err=0 0.32s']
- `13:30:15` intraday lane: not in a session window yet (Thu 09:30 NY)
- `13:30:15` ✗ timetable not confirmed: 40/41 fired at the new minute, 1 at the old, late ticks {'daily-morn-late': True, 'daily-morn-late2': True}, lane n/a
