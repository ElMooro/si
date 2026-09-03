# ops 5174 -- Universe Heatmap browser QA (1440 / 768 / 390)

**Status:** failure  
**Duration:** 222.9s  
**Finished:** 2026-09-03T22:31:41+00:00  

## Error

```
SystemExit: 1
```

## Log
## P0 deploy

- `22:31:00` ✅    live chart-pro.html carries the new workspace after 180s
## P1 render + hydration + modes/horizons at three widths

## P2-P7 editor, search, danger, keyboard reorder, touch reorder, migration, focus

## verdict

- `22:31:41` ✗    1440px P1: Page.evaluate: ReferenceError: Heatmap is not defined
    at eval (eval at evaluate (:311:30), <anonymous>:1:15)
    at UtilityScript.evaluate (<anonymous>:318:
- `22:31:41` ✗    768px P1: Page.evaluate: ReferenceError: Heatmap is not defined
    at eval (eval at evaluate (:311:30), <anonymous>:1:15)
    at UtilityScript.evaluate (<anonymous>:318:
- `22:31:41` ✗    390px P1: Page.evaluate: ReferenceError: Heatmap is not defined
    at eval (eval at evaluate (:311:30), <anonymous>:1:15)
    at UtilityScript.evaluate (<anonymous>:318:
- `22:31:41` ✗    P2-P7: Page.evaluate: ReferenceError: Heatmap is not defined
    at eval (eval at evaluate (:311:30), <anonymous>:1:15)
    at UtilityScript.evaluate (<anonymous>:318:18)
    at UtilityScript.<anonymous> (<a
- `22:31:41`    RED: 4 failure(s)
