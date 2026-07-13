# ops 3248 — fleet certification after the marathon

**Status:** failure  
**Duration:** 22.7s  
**Finished:** 2026-07-13T12:33:24+00:00  

## Error

```
SystemExit: 1
```

## Data

| feeds_fresh | functions | functions_with_errors | n_fails | n_warns | of | verdict |
|---|---|---|---|---|---|---|
|  | 653 |  |  |  |  |  |
|  |  | 9 |  |  |  |  |
| 9 |  |  |  |  | 9 |  |
|  |  |  | 1 | 0 |  | FAIL |

## Log
## 1. Errors metric, every function, 12h

- `12:33:18`   ⚠ TONIGHT justhodl-wl-engines: 27 err — 
- `12:33:18`     justhodl-consumer-pulse: 1 err — 
- `12:33:19`     justhodl-cb-injection: 1 err — 
- `12:33:19`     justhodl-theme-rotation-engine: 1 err — [ERROR] AttributeError: 'NoneType' object has no attribute 'get'
- `12:33:20`     justhodl-khalid-metrics: 1 err — 
- `12:33:20`     justhodl-boj-detail: 1 err — 
- `12:33:21`     justhodl-ka-metrics: 1 err — 
- `12:33:22`     justhodl-yen-carry: 1 err — 
- `12:33:22`     justhodl-snb-detail: 1 err — 
## 2. Tonight's deploys — explicit clean check

- `12:33:22`   dirty: justhodl-wl-engines
## 3. Feed freshness

- `12:33:23`   ✓ data/wl-engines.json               age=4.2h (cap 26h)
- `12:33:23`   ✓ data/wl-fusion.json                age=4.2h (cap 26h)
- `12:33:23`   ✓ data/thesis-engine.json            age=5.2h (cap 26h)
- `12:33:23`   ✓ data/credit-stress.json            age=0.5h (cap 26h)
- `12:33:23`   ✓ data/eurodollar-plumbing.json      age=0.5h (cap 26h)
- `12:33:23`   ✓ data/macro-nowcast.json            age=0.3h (cap 26h)
- `12:33:24`   ✓ data/crisis-composite.json         age=0.3h (cap 26h)
- `12:33:24`   ✓ data/market-internals.json         age=11.7h (cap 30h)
- `12:33:24`   ✓ data/symbol-map.json               age=4.2h (cap 30h)
- `12:33:24` ✗ justhodl-wl-engines: 27 errors post-deploy
