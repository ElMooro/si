# ops 3372 — hot-money Asia/Europe focus drilldowns

**Status:** success  
**Duration:** 844.0s  
**Finished:** 2026-07-17T03:43:43+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:34:41` FAIL  G1_deploy_settled_v140 — LastUpdateStatus=Successful
- `03:39:43` PASS  G2_fresh_v140_feed — version=1.4.0 at=2026-07-17T03:34:58
- `03:39:43` PASS  G3_core_asia_populated — {"Hong Kong": true, "Taiwan": true, "South Korea": true, "China": true}
- `03:39:43` PASS  G4_focus_breadth — europe=8['Germany', 'UK', 'France', 'Switzerland', 'Netherlands', 'Italy'] asia=7
- `03:39:43` PASS  G5_momentum_wired — 15/15 focus drills carry day_chg_pct
- `03:43:43` FAIL  G6_page_focus_markers — http 200
- `03:43:43` VERDICT: GAPS: G1_deploy_settled_v140,G6_page_focus_markers
