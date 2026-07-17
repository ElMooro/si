# ops 3388 — GSSI math v2 + barometer

**Status:** success  
**Duration:** 858.9s  
**Finished:** 2026-07-17T15:59:50+00:00  

## Error

```
SystemExit: 0
```

## Log
- `15:50:38` FAIL  G1_engine_240_settled — markers in zip
- `15:59:50` FAIL  G2_v2_feed — latest={} pts=0 bc_tail_ok=[]
- `15:59:50` FAIL  G3_detection — detected=0/14 (v1: 8/14) must_ok=False leads_v1_to_v2={"Lehman": {"v1": 198, "v2": null}, "COVID crash": {"v1": -10, "v2": null}, "Euro debt crisis (IT/ES)": {"v1": 61, "v2": null}} improved_or_equal=0/3
- `15:59:50` PASS  G4_gauge_live — missing=[]
- `15:59:50` VERDICT: GAPS: G1_engine_240_settled,G2_v2_feed,G3_detection
