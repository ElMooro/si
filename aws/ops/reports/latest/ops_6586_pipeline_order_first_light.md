# ops 6586 - pipeline order, first light: did the retimed triggers fire at their new minutes?

**Status:** failure  
**Duration:** 3.5s  
**Finished:** 2026-10-21T23:36:21+00:00  

## Error

```
SystemExit: 1
```

## Data

| due | errors | fired_new | fired_old | lane | late_ticks_checked | late_ticks_ok |
|---|---|---|---|---|---|---|
| 75 | 0 | 72 | 3 | not as planned | 1 | 1 |

## Log
- `23:36:21` retimed triggers due so far today: 75; fired at the new minute: 72; still fired at the old minute: 3
- `23:36:21` ⚠ no invocation in the 6 min from its new time: justhodl-auction-desk-buyback cron(51 13 ? * MON-FRI *)
- `23:36:21` ⚠ no invocation in the 6 min from its new time: justhodl-master-ranker-weekday-close cron(5 17 ? * MON-FRI *)
- `23:36:21` ⚠ no invocation in the 6 min from its new time: justhodl-bond-warroom-after-auction cron(56 13 ? * MON-FRI *)
- `23:36:21` late tick daily-morn-late: members invoked after cron(15 11 * * ? *): {"justhodl-morning-brief-tg": 1.0, "justhodl-nobrainer-rationale": 1.0, "justhodl-sector-earnings-diffusion": 1.0}; scheduler log: ['[scheduler] tick=daily-morn-late ok=3 err=0 0.4s']
- `23:36:21` intraday lane: expected ~96 runs each since 09:00 NY; invocations {"justhodl-edge-engine": 96.0, "justhodl-implied-prob": 96.0, "justhodl-intraday-pulse": 97.0, "justhodl-market-map": 97.0, "justhodl-opex-calendar": 96.0, "justhodl-options-analytics": 97.0, "justhodl-options-gamma": 97.0, "justhodl-polygon-futures-curves": 196.0, "justhodl-polygon-fx-regime": 97.0, "justhodl-risk-regime": 96.0, "justhodl-tail-risk": 96.0, "justhodl-trade-ticket-monitor": 97.0}; errors {"justhodl-polygon-futures-curves": 98.0} -> NOT AS PLANNED
- `23:36:21` ✗ timetable not confirmed: 72/75 fired at the new minute, 3 at the old, late ticks {'daily-morn-late': True}, lane expected ~96 runs each since 09:00 NY; invocations {"justhodl-edge-engine": 96.0, "justhodl-implied-prob": 96.0, "justhodl-intraday-pulse": 97.0, "justhodl-market-map": 97.0, "justhodl-opex-calendar": 96.0, "justhodl-options-analytics": 97.0, "justhodl-options-gamma": 97.0, "justhodl-polygon-futures-curves": 196.0, "justhodl-polygon-fx-regime": 97.0, "justhodl-risk-regime": 96.0, "justhodl-tail-risk": 96.0, "justhodl-trade-ticket-monitor": 97.0}; errors {"justhodl-polygon-futures-curves": 98.0}
