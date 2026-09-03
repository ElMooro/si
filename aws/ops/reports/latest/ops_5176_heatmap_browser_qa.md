# ops 5176 -- Universe Heatmap browser QA (1440 / 768 / 390)

**Status:** success  
**Duration:** 254.7s  
**Finished:** 2026-09-03T22:49:51+00:00  

## Data

| cells | errors | hydrated | overflow | section | sections | width |
|---|---|---|---|---|---|---|
| 64 | 0 | 18 | 0 | P1 | 6 | 1440 |
| 64 | 0 | 51 | 0 | P1 | 6 | 768 |
| 64 | 0 | 22 | 0 | P1 | 6 | 390 |

## Log
## P0 deploy

- `22:48:54` ✅    live chart-pro.html carries the new workspace after 195s
- `22:49:13` ✅    main script parses in Chrome (Heatmap + MacroData defined): True
## P1 render + hydration + modes/horizons at three widths

- `22:49:21`    1440px: sections=6 cells=64 hydrated=18 first D=+1.25% W=+6.94% legend='0200cascade combined score' bg-variety=7 stored.version=3 overflow=0px errors=0
- `22:49:29`     768px: sections=6 cells=64 hydrated=51 first D=+1.25% W=+6.94% legend='0200cascade combined score' bg-variety=9 stored.version=3 overflow=0px errors=0
- `22:49:36`     390px: sections=6 cells=64 hydrated=22 first D=+1.25% W=+6.94% legend='0200cascade combined score' bg-variety=9 stored.version=3 overflow=0px errors=0
## P2-P7 editor, search, danger, keyboard reorder, touch reorder, migration, focus

- `22:49:41` ✅    P6 migration v2->v3: symbols=['NVDA', 'AMD'] mode=signals horizon=M
- `22:49:42` ✅    P2 category added=True inert-while-editing=True restored=True
- `22:49:44` ✅    P2 search results=33 selected='GLD · SPDR Gold Shares' added=GLD
- `22:49:48`    P3 danger cell GLD -> {'state': 'ready', 'danger': True, 'ch': '+1.85%'}
- `22:49:49` ✅    P4 keyboard reorder ['GLD', 'SPY', 'QQQ'] -> ['SPY', 'GLD', 'QQQ'] (focus on macro-drag-handle)
- `22:49:50` ✅    P5 touch reorder ['SPY', 'GLD', 'QQQ', 'IWM'] -> ['GLD', 'QQQ', 'SPY', 'IWM']
- `22:49:51` ✅    P7 Escape: editor closed=True modal stayed=True then closed=True focus->hm-add-instrument
## verdict

- `22:49:51` ✅    GREEN: heatmap workspace renders and hydrates at 1440/768/390, modes/horizons repaint, editor + search + danger + keyboard/touch reorder + migration + focus all pass
