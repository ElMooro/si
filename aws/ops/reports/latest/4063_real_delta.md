# ops 4063 — the delta on REAL attribution

**Status:** failure  
**Duration:** 211.1s  
**Finished:** 2026-07-29T00:40:12+00:00  

## Error

```
SystemExit: 1
```

## Data

| fnerr | junk_filtered | real | settled | symbols_with_notes | symbols_with_tv_source | total | unique_symbols | watchlists |
|---|---|---|---|---|---|---|---|---|
|  | 1000 | 0 |  |  |  | 1000 |  |  |
| None |  |  | False | 771 | 0 |  | 10319 | 491 |

## Log
## KNOWN families covered

## NEW — sources the system does NOT have

- `00:36:41` ✅   data/source-map.json v1.1 written (real only)
## workbench v1.2 settle + rejoin

- `00:40:12` ✗   real >= 200
- `00:40:12` ✗   delta has >= 3 NEW sources
- `00:40:12` ✗   workbench v1.2 settled+clean
- `00:40:12` ✗   page joins real attribution (>=100 sourced)
- `00:40:12` ✗ FAILED: ['real >= 200', 'delta has >= 3 NEW sources', 'workbench v1.2 settled+clean', 'page joins real attribution (>=100 sourced)']
