# ops 3847 — industry exposure rendered + portwatch untouched

**Status:** failure  
**Duration:** 120.5s  
**Finished:** 2026-07-25T03:09:54+00:00  

## Error

```
SystemExit: 1
```

## Data

| divergent | in_nav | keys_missing | markers_missing | page_bytes |
|---|---|---|---|---|
| 5 | True | 3 | 0 | 18741 |

## Log
## 1. Served at the edge

- `03:07:53`   attempt 1: 13,980 bytes, marker absent
- `03:08:13`   attempt 2: 13,980 bytes, marker absent
- `03:08:34`   attempt 3: 13,980 bytes, marker absent
- `03:08:54`   attempt 4: 13,980 bytes, marker absent
- `03:09:14`   attempt 5: 13,980 bytes, marker absent
- `03:09:34`   attempt 6: 13,980 bytes, marker absent
- `03:09:54` ✅   served on attempt 7 (18,741 bytes)
## 2. portwatch.html must be UNTOUCHED

- `03:09:54`   served portwatch.html = 8,606 bytes
- `03:09:54`   repo copy 7,132 bytes · identical=False
- `03:09:54` ✅   original page still serving its own title — untouched
## 3. Field coverage vs BOTH live feeds

- `03:09:54`   keys checked 32 · missing 3
- `03:09:54` ✗   NO RENDER PATH: ['n_industries', 'port_yoy_pct', 'top_industry']
## 4. Structural markers

- `03:09:54` ✅   industry rollup
- `03:09:54` ✅   per-port breakdown
- `03:09:54` ✅   not-a-prediction framing
- `03:09:54` ✅   coverage stated
- `03:09:54` ✅   divergence board
- `03:09:54` ✅   country table
- `03:09:54` ✅   chokepoints
- `03:09:54` ✅   all ports
- `03:09:54` ✅   coverage gaps
- `03:09:54` ✅   limits shipped
- `03:09:54` ✅   links to old page
## 5. Nav (served manifest)

- `03:09:54` ✅   listed under 'Macro & Liquidity'
- `03:09:54` ✅   portwatch.html still listed under 'Macro & Liquidity'
- `03:09:54` ✗ FAILED — keys ['n_industries', 'port_yoy_pct', 'top_industry'] · markers []
