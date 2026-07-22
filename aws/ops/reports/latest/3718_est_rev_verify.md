# ops 3718 — verify estimate-revisions v2.1.0 live + directions populate

**Status:** failure  
**Duration:** 707.4s  
**Finished:** 2026-07-22T19:08:43+00:00  

## Error

```
SystemExit: 1
```

## Data

| detail | gate | ok |
|---|---|---|
| fmp_estimate_profile() returns fwd_eps_cur (present=True) | G0_key_contract | True |
| fmp populated at char 746 before row loop at 5093 | G0b_fmp_ready_before_loop | True |
| source carries the FMP fallback + v2.1.0 (applied by ops 3717) | G1_patch_committed | True |
| v2.1.0 zip proven live after 707s (3717 timed out at 424s waiting on the parallel deploy-lambdas run) | G2_artifact_live | False |

## Log
- `18:56:56` G0_key_contract True
- `18:56:56` G0b_fmp_ready_before_loop True
- `18:56:56` G1_patch_committed True
- `19:08:43` G2_artifact_live False
- `19:08:43` VERDICT: GAPS: G2_artifact_live
