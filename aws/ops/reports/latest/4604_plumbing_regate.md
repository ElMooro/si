# ops 4604 — plumbing v2.0.1 regate (supersedes 4603)

**Status:** failure  
**Duration:** 60.6s  
**Finished:** 2026-08-11T19:58:08+00:00  

## Error

```
SystemExit: 1
```

## Data

| invoke_composite | invoke_label | invoke_n_with_data |
|---|---|---|
| 48.0 | NORMAL | 45 |

## Log
## deploy-settle on the v2.0.1 marker

- `19:57:09` zip carries v2.0.1 (attempt 1)
- `19:57:09` ✅   [deploy] deployed zip carries v2.0.1 + NYFED_RATES
## warm gap-fill: TGCR/BGCR from the NY Fed (keyless)

- `19:57:09` ⚠ TGCR: HTTP Error 400: Bad Request
- `19:57:09` ⚠ BGCR: HTTP Error 400: Bad Request
- `19:57:09` banked: none
- `19:57:09` ✗   [warm] CONTRACT MISS — TGCR + BGCR banked to the warm store (data.html now carries all 17 L0 series)
## invoke v2.0.1 + payload contract (proper parse)

- `19:58:07` ✅   [invoke] engine returned ok:true (parsed)
- `19:58:07` ✅   [schema] schema 2.0
- `19:58:07` ✗   [L0-cov] CONTRACT MISS — L0 coverage >=18 with TGCR/BGCR live (got 16/23)
- `19:58:07` ✗   [TGCR] CONTRACT MISS — TGCR carrying data (value=None)
- `19:58:07` ✗   [BGCR] CONTRACT MISS — BGCR carrying data (value=None)
- `19:58:07` ✗   [SOFR_TGCR_BP] CONTRACT MISS — SOFR_TGCR_BP carrying data (value=None)
- `19:58:07` ✅   [L1-regress] L1 still scoring (20.3)
- `19:58:07` ✅   [L2-regress] L2 still scoring (70.8)
- `19:58:07` ✅   [L3-regress] L3 still scoring (47.1)
- `19:58:07` ✅   [L4-regress] L4 still scoring (49.5)
- `19:58:07` ✅   [composite] composite 48.0 (NORMAL)
## edge: payload freshness past the 600s TTL

- `19:58:08` edge serving schema 2.0 (attempt 1, as_of=2026-08-11T19:57:19+00:00)
- `19:58:08` ✅   [edge-payload] served plumbing-stress.json is schema 2.0
- `19:58:08` ✅   [edge-page] page still carries L0 card + concept map
## verdict

- `19:58:08` ✗ regate: 5 red (CF token 401 remains a KHALID action item)
