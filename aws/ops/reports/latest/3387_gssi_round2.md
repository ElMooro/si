# ops 3387 — SG/HK retry + Turkey/Argentina

**Status:** success  
**Duration:** 113.5s  
**Finished:** 2026-07-17T15:28:55+00:00  

## Error

```
SystemExit: 0
```

## Log
- `15:27:14` PASS  G1_engine_230_settled — markers in zip
- `15:28:55` FAIL  G2_sg_hk — SG_bp=None HK_bp=None errs=[yoy/singapore: MAS SGS empty | yoy/hong_kong: HKMA empty]
- `15:28:55` PASS  G3_tr_ar_on_desk — TR=68.6 (CDS 228.61bp) AR=10.0 (CDS Nonebp)
- `15:28:55` PASS  G4_gssi_regen — gen_fresh=True turkey_in=True detected=8/14 now={"date": "2026-07-10", "gssi": 39.19, "pctile": 19.1, "yoy_pct": -6.2, "d6m": -0
- `15:28:55` VERDICT: GAPS: G2_sg_hk
