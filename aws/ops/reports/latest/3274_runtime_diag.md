## 1. Served drawer JS parses?

**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-07-13T19:43:48+00:00  

## Data

| block_bytes | block_parses | drawer_bytes | drawer_parses | n_fails | verdict |
|---|---|---|---|---|---|
|  |  | 16165 | True |  |  |
| 7008 | True |  |  |  |  |
|  |  |  |  | 0 | PASS |

## Log
## 2. Served chart-pro block parses?

## 3. Data sources — worker vs same-origin

- `19:43:47`   worker tv-watchlists.json     200    229390B items=207
- `19:43:47`   worker symbol-map.json        200    591449B items=4823
- `19:43:47`   origin tv-watchlists.json     200    229390B items=207
- `19:43:48`   origin symbol-map.json        200    591449B items=4823
## 4. What a plain (unbusted) user request gets

- `19:43:48`   home             cf=DYNAMIC age=0 cc=max-age=600 tag=jh-nav-drawer.js?v=94b10af2
- `19:43:48`   chart-pro.html   cf=DYNAMIC age=1 cc=max-age=600 tag=jh-nav-drawer.js?v=94b10af2
