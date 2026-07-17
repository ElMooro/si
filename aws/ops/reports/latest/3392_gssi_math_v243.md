# ops 3388 — GSSI math v2 + barometer

**Status:** success  
**Duration:** 128.5s  
**Finished:** 2026-07-17T16:36:50+00:00  

## Error

```
SystemExit: 0
```

## Log
- `16:35:07` PASS  G1_engine_240_settled — markers in zip
- `16:36:50` FAIL  G2_v2_feed — latest={"date": "2026-07-10", "gssi": 36.08, "pctile": 32.5, "breadth_pct": 0.0, "comove": 0.0, "yoy_pct": -4.0, "d6m": -2.4} pts=1715 bc_tail_ok=False
- `16:36:50` PASS  G3_detection — detected=8/14 (v1: 8/14) must_ok=True leads_v1_to_v2={"Lehman": {"v1": 198, "v2": 262}, "COVID crash": {"v1": -10, "v2": -10}, "Euro debt crisis (IT/ES)": {"v1": 61, "v2": 224}} improved_or_equal=3/3
- `16:36:50` PASS  G4_gauge_live — missing=[]
- `16:36:50` VERDICT: GAPS: G2_v2_feed
