# ops 5176 -- Universe Heatmap browser QA (1440 / 768 / 390)

**Status:** failure  
**Duration:** 25.1s  
**Finished:** 2026-09-03T22:37:33+00:00  

## Error

```
SystemExit: 1
```

## Log
## P0 deploy

- `22:37:08` ✅    live chart-pro.html carries the new workspace after 0s
## P1 render + hydration + modes/horizons at three widths

## P2-P7 editor, search, danger, keyboard reorder, touch reorder, migration, focus

## verdict

- `22:37:33` ✗    1440px P1: Page.evaluate: ReferenceError: Heatmap is not defined
    at eval (eval at evaluate (:311:30), <anonymous>:1:15)
    at UtilityScript.evaluate (<anonymous>:318:
- `22:37:33` ✗    768px P1: Page.evaluate: ReferenceError: Heatmap is not defined
    at eval (eval at evaluate (:311:30), <anonymous>:1:15)
    at UtilityScript.evaluate (<anonymous>:318:
- `22:37:33` ✗    390px P1: Page.evaluate: ReferenceError: Heatmap is not defined
    at eval (eval at evaluate (:311:30), <anonymous>:1:15)
    at UtilityScript.evaluate (<anonymous>:318:
- `22:37:33` ✗    P2-P7: Page.evaluate: ReferenceError: Heatmap is not defined
    at eval (eval at evaluate (:311:30), <anonymous>:1:15)
    at UtilityScript.evaluate (<anonymous>:318:18)
    at UtilityScript.<anonymous> (<a
- `22:37:33`    RED: 4 failure(s)
