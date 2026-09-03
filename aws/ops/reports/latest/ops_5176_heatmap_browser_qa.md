# ops 5176 -- Universe Heatmap browser QA (1440 / 768 / 390)

**Status:** failure  
**Duration:** 52.3s  
**Finished:** 2026-09-03T22:44:27+00:00  

## Error

```
SystemExit: 1
```

## Data

| cells | errors | hydrated | overflow | section | sections | width |
|---|---|---|---|---|---|---|
| 64 | 0 | 7 | 0 | P1 | 6 | 1440 |
| 64 | 0 | 52 | 0 | P1 | 6 | 768 |
| 64 | 0 | 22 | 0 | P1 | 6 | 390 |

## Log
## P0 deploy

- `22:43:36` ✅    live chart-pro.html carries the new workspace after 0s
- `22:43:50` ✅    main script parses in Chrome (Heatmap + MacroData defined): True
## P1 render + hydration + modes/horizons at three widths

- `22:43:58`    1440px: sections=6 cells=64 hydrated=7 first D=+1.25% W=+6.94% legend='0200cascade combined score' bg-variety=9 stored.version=3 overflow=0px errors=0
- `22:44:06`     768px: sections=6 cells=64 hydrated=52 first D=+1.25% W=+6.94% legend='0200cascade combined score' bg-variety=9 stored.version=3 overflow=0px errors=0
- `22:44:13`     390px: sections=6 cells=64 hydrated=22 first D=+1.25% W=+6.94% legend='0200cascade combined score' bg-variety=9 stored.version=3 overflow=0px errors=0
## P2-P7 editor, search, danger, keyboard reorder, touch reorder, migration, focus

- `22:44:18` ✅    P6 migration v2->v3: symbols=['NVDA', 'AMD'] mode=signals horizon=M
- `22:44:19` ✅    P2 category added=True inert-while-editing=True restored=True
- `22:44:21` ✅    P2 search results=33 selected='GLD · SPDR Gold Shares' added=GLD
- `22:44:25`    P3 danger cell GLD -> {'state': 'ready', 'danger': True, 'ch': '+1.85%'}
- `22:44:26` ✅    P4 keyboard reorder ['GLD', 'SPY', 'QQQ'] -> ['SPY', 'GLD', 'QQQ'] (focus on macro-drag-handle)
- `22:44:27` ⚠    P5 touch reorder ['SPY', 'GLD', 'QQQ', 'IWM'] -> ['SPY', 'GLD', 'QQQ', 'IWM']
- `22:44:27` ✅    P7 Escape: editor closed=True modal stayed=True then closed=True focus->hm-add-instrument
## verdict

- `22:44:27` ✗    P5 touch reorder (synthetic pointer events)
- `22:44:27` ✗    desktop QA page errors: ["Failed to execute 'setPointerCapture' on 'Element': No active pointer with the given id is found."]
- `22:44:27`    RED: 2 failure(s)
