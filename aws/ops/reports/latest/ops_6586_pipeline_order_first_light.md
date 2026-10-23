# ops 6586 - pipeline order, first light: did the retimed triggers fire at their new minutes?

**Status:** success  
**Duration:** 5.3s  
**Finished:** 2026-10-23T13:29:37+00:00  

## Data

| due | errors | fired_new | fired_old | lane | late_ticks_checked | late_ticks_ok |
|---|---|---|---|---|---|---|
| 41 | 2 | 41 | 0 | n/a | 2 | 2 |

## Log
- `13:29:36` retimed triggers due so far today: 41; fired at the new minute: 41; still fired at the old minute: 0
- `13:29:36` ⚠ errors after the retime: justhodl-estimate-revisions 3
- `13:29:36` ⚠ errors after the retime: justhodl-research-backtest 2
- `13:29:37` late tick daily-morn-late: members invoked after cron(15 11 * * ? *): {"justhodl-khalid-metrics": 1.0, "justhodl-momentum-breakout": 1.0, "justhodl-morning-brief-tg": 1.0, "justhodl-nobrainer-rationale": 1.0, "justhodl-pre-pump-detector": 1.0, "justhodl-revenue-acceleration": 1.0}; scheduler log: ['[scheduler] tick=daily-morn-late ok=6 err=0 0.47s', '[scheduler] tick=daily-morn-late2 ok=3 err=0 0.33s']
- `13:29:37` late tick daily-morn-late2: members invoked after cron(30 11 * * ? *): {"justhodl-sector-earnings-diffusion": 1.0, "justhodl-theme-rotation-engine": 1.0, "justhodl-volatility-squeeze-hunter": 1.0}; scheduler log: ['[scheduler] tick=daily-morn-late2 ok=3 err=0 0.33s']
- `13:29:37` intraday lane: not in a session window yet (Fri 09:29 NY)
- `13:29:37` ✅ first light: 41/41 retimed triggers fired at their new minutes, none at the old, late tick(s) fired
