# ops 3819 — rotation-dashboard.html edge verify + field coverage

**Status:** failure  
**Duration:** 101.1s  
**Finished:** 2026-07-24T21:01:56+00:00  

## Error

```
SystemExit: 1
```

## Data

| degraded | in_nav | keys_missing | keys_rendered | markers_missing | page_bytes |
|---|---|---|---|---|---|
| NONE | True | 9 | 64 | 0 | 22495 |

## Log
## 1. Served page (Cloudflare edge, unique marker)

- `21:00:15`   attempt 1: HTTP Error 404: Not Found
- `21:00:35`   attempt 2: HTTP Error 404: Not Found
- `21:00:55`   attempt 3: HTTP Error 404: Not Found
- `21:01:15`   attempt 4: HTTP Error 404: Not Found
- `21:01:35`   attempt 5: HTTP Error 404: Not Found
- `21:01:55` ✅   marker 'v1-ops3819' served on attempt 6 (22,495 bytes)
## 2. FIELD-COVERAGE AUDIT — live artifact vs served html

- `21:01:56`   keys checked: 73 · rendered: 64
- `21:01:56` ✗   NO RENDER PATH (9): ['above_200d_sma', 'horizon', 'key', 'prev_rank', 'regime_prior', 'ret_12m_pct', 'ret_1m_pct', 'sma_200d', 'thesis']
## 3. Structural markers

- `21:01:56` ✅   regime banner
- `21:01:56` ✅   four-layer strip
- `21:01:56` ✅   overweight board
- `21:01:56` ✅   RRG scatter
- `21:01:56` ✅   ratio table
- `21:01:56` ✅   ranked table
- `21:01:56` ✅   avoid board
- `21:01:56` ✅   methodology
- `21:01:56` ✅   gold caveat copy
- `21:01:56` ✅   gate explainer
- `21:01:56` ✅   degraded surfaced
- `21:01:56` ✅   cot not-applied flag
## 4. Nav manifest (served)

- `21:01:56` ✅   listed under 'Portfolio & Execution' as 'Rotation Dashboard'
- `21:01:56` ✗ FAILED — missing keys ['above_200d_sma', 'horizon', 'key', 'prev_rank', 'regime_prior', 'ret_12m_pct', 'ret_1m_pct', 'sma_200d', 'thesis'] · missing markers []
