# ops 3717 — estimate-revisions cur_eps re-sourced onto FMP

**Status:** failure  
**Duration:** 423.8s  
**Finished:** 2026-07-22T18:54:39+00:00  

## Error

```
SystemExit: 1
```

## Data

| detail | gate | ok |
|---|---|---|
| fmp_estimate_profile() returns fwd_eps_cur (present=True) | G0_key_contract | True |
| fmp populated at char 746 before row loop at 4740 | G0b_fmp_ready_before_loop | True |
| cur_eps falls back to FMP fwd_eps_cur; version -> 2.1.0 | G1_patch_applied | True |
| patched zip proven live after 424s | G2_artifact_live | False |

## Log
- `18:47:35` G0_key_contract True
- `18:47:35` G0b_fmp_ready_before_loop True
- `18:47:35` G1_patch_applied True
- `18:54:39` G2_artifact_live False
- `18:54:39` VERDICT: GAPS: G2_artifact_live
