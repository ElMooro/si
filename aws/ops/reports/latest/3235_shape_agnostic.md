# ops 3235 — shape-agnostic browse-then-probe

**Status:** success  
**Duration:** 33.8s  
**Finished:** 2026-07-13T07:12:29+00:00  

## Data

| curations | n_fails | n_warns | verdict |
|---|---|---|---|
| 0 |  |  |  |
|  | 0 | 1 | PASS |

## Log
## 0. Endpoint skeletons (for the record)

- `07:11:59`   datasets/Eurostat/ei_bssi_m_r2: {"_meta": {"args": "dict", "version": "str"}, "datasets": {"docs": "list", "limit": "int", "num_found": "int", "offset": "int"}}
- `07:12:01`   search?q=ifo&limit=2: {"_meta": {"args": "dict", "version": "str"}, "results": {"docs": "list", "limit": "int", "num_found": "int", "offset": "int"}}
## 1-4. Search-probe the four blockers

- `07:12:07`   [ECONOMICS:EUBCOI] 0 candidates
- `07:12:09`   [ECONOMICS:EUMPRYY] 0 candidates
- `07:12:26`   [ECONOMICS:DEIFOE] 0 candidates
- `07:12:29`   [ECONOMICS:DEZCC] 0 candidates
- `07:12:29` ⚠ nothing landed — skeletons above show what the API actually returns
