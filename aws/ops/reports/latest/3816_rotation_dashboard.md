# ops 3816 — rotation-dashboard v1.0.0 (cross-asset spine)

**Status:** failure  
**Duration:** 0.4s  
**Finished:** 2026-07-24T20:12:15+00:00  

## Error

```
SystemExit: 1
```

## Log
## G0. KEY CONTRACT — grep producers before consuming

- `20:12:14` ✅   nowcast_quadrant: 'nowcast_quadrant' present in justhodl-nowcast-desk
- `20:12:14` ✅   risk-regime score: 'score' present in justhodl-risk-regime
- `20:12:14` ✅   dollar chg_3m_pct: 'chg_3m_pct' present in justhodl-dollar-radar
- `20:12:14` ✅ G0 PASS — every consumed key exists in its producer
## 1. Inherit env from donor

- `20:12:15` ✗ donor justhodl-confluence-meta has no POLYGON_API_KEY — engine cannot build history
