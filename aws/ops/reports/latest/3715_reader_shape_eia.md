# ops 3715 — readthrough reader shape fix + EIA re-gate

**Status:** success  
**Duration:** 36.3s  
**Finished:** 2026-07-22T17:58:17+00:00  

## Data

| detail | gate | ok |
|---|---|---|
| reader tuple += all_results, top_25_by_score; VERSION -> 1.2.2 | A1_patch_applied | True |
| v1.2.2 zip-proven live after 0s | A2_artifact_live | True |
| artifact refreshed in 15s | A3_refreshed | True |
| degraded=['fundamental sidecar missing: estimate-revisions'] (forward-orders entry must be GONE) | A4_sidecar_joined | True |
| rows carrying forward-orders-derived fields: 4/84 | A5_fwd_fields_present | True |
| FunctionError=None eia_key_present=True steo_keys=[] steo_error=None | B1_eia_key_live | True |

## Log
## A — readthrough reader shape

- `17:57:40` A1_patch_applied True
- `17:57:41`   zip: 102021 bytes
## 1. Lambda

- `17:57:41`   Lambda exists — updating
- `17:57:44` ✅   ✓ updated justhodl-readthrough
- `17:57:44` A2_artifact_live True
- `17:57:59` A3_refreshed True
- `17:57:59` A4_sidecar_joined True
- `17:57:59` A5_fwd_fields_present True
## B — EIA re-gate

- `17:58:17` B1_eia_key_live True
- `17:58:17` VERDICT: PASS_ALL
