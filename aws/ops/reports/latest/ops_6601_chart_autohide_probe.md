# ops 6601 - chart.html auto-hide probe (headless Chrome on the runner, read-only)

**Status:** success  
**Duration:** 57.8s  
**Finished:** 2026-10-21T16:48:28+00:00  

## Data

| bot | hidden_on_load | jump_hidden | left | profile | right | top | verdict |
|---|---|---|---|---|---|---|---|
| True/True | True | True | True/True | fresh | True/True | True/True | OK |
| True/True | True | True | True/True | old-persistent-pin | True/True | True/True | OK |

## Log
- `16:47:31` served chart.html (build ?) references /jh-chart-autohide.js?v=7e0dbbd4; that file carries the fix: True
- `16:47:31` installing playwright
- `16:47:55` [fresh] on load: auto-hide True, pin_ls None pin_ss '1', desks 22; sheets {"top": {"show": false, "top": -250, "left": 0, "right": 1600, "bottom": -2, "w": 1600, "h": 248}, "left": {"show": false, "top": 0, "left": -54, "right": -2, "bottom": 1000, "w": 52, "h": 1000}, "right": {"show": false, "top": 0, "left": 1602, "right": 1642, "bottom": 1000, "w": 40, "h": 1000}, "bot": {"show": false, "top": 1002, "left": 0, "right": 1600, "bottom": 1061, "w": 1600, "h": 59}}
- `16:47:56` [fresh] top   edge -> shown True | away -> hidden True ({"show": false, "top": -250, "left": 0, "right": 1600, "bottom": -2, "w": 1600, "h": 248})
- `16:47:58` [fresh] right edge -> shown True | away -> hidden True ({"show": false, "top": 0, "left": 1602, "right": 1642, "bottom": 1000, "w": 40, "h": 1000})
- `16:48:00` [fresh] left  edge -> shown True | away -> hidden True ({"show": false, "top": 0, "left": -54, "right": -2, "bottom": 1000, "w": 52, "h": 1000})
- `16:48:01` [fresh] bot   edge -> shown True | away -> hidden True ({"show": false, "top": 1002, "left": 0, "right": 1600, "bottom": 1061, "w": 1600, "h": 59})
- `16:48:03` [fresh] reveal top then jump to the middle: hidden again True
- `16:48:20` [old-persistent-pin] on load: auto-hide True, pin_ls None pin_ss '1', desks 22; sheets {"top": {"show": false, "top": -250, "left": 0, "right": 1600, "bottom": -2, "w": 1600, "h": 248}, "left": {"show": false, "top": 0, "left": -54, "right": -2, "bottom": 1000, "w": 52, "h": 1000}, "right": {"show": false, "top": 0, "left": 1602, "right": 1642, "bottom": 1000, "w": 40, "h": 1000}, "bot": {"show": false, "top": 1002, "left": 0, "right": 1600, "bottom": 1061, "w": 1600, "h": 59}}
- `16:48:22` [old-persistent-pin] top   edge -> shown True | away -> hidden True ({"show": false, "top": -250, "left": 0, "right": 1600, "bottom": -2, "w": 1600, "h": 248})
- `16:48:23` [old-persistent-pin] right edge -> shown True | away -> hidden True ({"show": false, "top": 0, "left": 1602, "right": 1642, "bottom": 1000, "w": 40, "h": 1000})
- `16:48:25` [old-persistent-pin] left  edge -> shown True | away -> hidden True ({"show": false, "top": 0, "left": -54, "right": -2, "bottom": 1000, "w": 52, "h": 1000})
- `16:48:27` [old-persistent-pin] bot   edge -> shown True | away -> hidden True ({"show": false, "top": 1002, "left": 0, "right": 1600, "bottom": 1061, "w": 1600, "h": 59})
- `16:48:28` [old-persistent-pin] reveal top then jump to the middle: hidden again True
- `16:48:28` ✅ bars hidden on load, revealed at their edge, hidden again when the pointer leaves - fresh profile and old-pin profile alike
