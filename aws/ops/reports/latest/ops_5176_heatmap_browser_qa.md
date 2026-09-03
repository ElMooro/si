# ops 5176 -- Universe Heatmap browser QA (1440 / 768 / 390)

**Status:** failure  
**Duration:** 234.9s  
**Finished:** 2026-09-03T22:42:26+00:00  

## Error

```
SystemExit: 1
```

## Data

| cells | errors | hydrated | overflow | section | sections | width |
|---|---|---|---|---|---|---|
| 64 | 0 | 9 | 0 | P1 | 6 | 1440 |
| 64 | 0 | 20 | 0 | P1 | 6 | 768 |
| 64 | 0 | 22 | 0 | P1 | 6 | 390 |

## Log
## P0 deploy

- `22:41:32` ✅    live chart-pro.html carries the new workspace after 180s
- `22:41:39` ✅    main script parses in Chrome (Heatmap + MacroData defined): True
## P1 render + hydration + modes/horizons at three widths

- `22:41:59`    1440px: sections=6 cells=64 hydrated=9 first D=+1.25% W=+6.94% legend='0200cascade combined score' bg-variety=3 stored.version=3 overflow=0px errors=0
- `22:42:06`     768px: sections=6 cells=64 hydrated=20 first D=+1.25% W=+6.94% legend='0200cascade combined score' bg-variety=9 stored.version=3 overflow=0px errors=0
- `22:42:13`     390px: sections=6 cells=64 hydrated=22 first D=+1.25% W=+6.94% legend='0200cascade combined score' bg-variety=9 stored.version=3 overflow=0px errors=0
## P2-P7 editor, search, danger, keyboard reorder, touch reorder, migration, focus

- `22:42:19` ✅    P6 migration v2->v3: symbols=['NVDA', 'AMD'] mode=signals horizon=M
- `22:42:20` ✅    P2 category added=True inert-while-editing=True restored=True
- `22:42:22` ✅    P2 search results=42 selected='FRED:GOLDAMGBD228NLBM · GOLDAMGBD228NLBM' added=FRED:GOLDAMGBD228NLBM
- `22:42:26`    P3 danger cell FRED:GOLDAMGBD228NLBM -> {'state': 'error', 'danger': False, 'ch': '—'}
- `22:42:26` ⚠    P3: added instrument not hydrated in time ({'state': 'error', 'danger': False, 'ch': '—'})
## verdict

- `22:42:26` ✗    P2-P7: 'NoneType' object is not subscriptable
- `22:42:26`    RED: 1 failure(s)
