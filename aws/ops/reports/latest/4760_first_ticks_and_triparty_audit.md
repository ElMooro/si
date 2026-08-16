# ops 4760 -- first ticks + triparty completeness + data.html

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-08-16T17:50:32+00:00  

## Data

| check | held_FNYR_BGCR_A | held_FNYR_BGCR_UV_A | held_FNYR_SOFR_UV_A | held_FNYR_TGCR_A | held_FNYR_TGCR_UV_A | page_nyfed | page_nyfed-research | page_ofr | page_ofr-bsrm | page_ofr-fsi | page_ofr-hfm | page_ofr-site | value |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| soma_fired |  |  |  |  |  |  |  |  |  |  |  |  | False |
| v2_ran |  |  |  |  |  |  |  |  |  |  |  |  | False |
| v2_summary_as_of |  |  |  |  |  |  |  |  |  |  |  |  | 2026-08-16T05:22:42+00:00 |
| hfm_state |  |  |  |  |  |  |  |  |  |  |  |  | absent |
| hfm_series_objects |  |  |  |  |  |  |  |  |  |  |  |  | 497 |
| ofr_TRI_mnemonics |  |  |  |  |  |  |  |  |  |  |  |  | 80 |
| ofr_GCF_mnemonics |  |  |  |  |  |  |  |  |  |  |  |  | 48 |
|  |  |  |  | True |  |  |  |  |  |  |  |  |  |
|  | True |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | True |  |  |  |  |  |  |  |  |
|  |  | True |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | True |  |  |  |  |  |  |  |  |  |  |
| catalog_generated_at |  |  |  |  |  |  |  |  |  |  |  |  | 2026-08-16T16:48:53+00:00 |
|  |  |  |  |  |  |  |  | keys=482 mb=None |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | keys=497 mb=None |  |  |
|  |  |  |  |  |  |  |  |  | keys=500 mb=None |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | keys=2 mb=None |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | keys=5 mb=None |  |
|  |  |  |  |  |  | keys=1604 mb=None |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | NOT ON PAGE YET |  |  |  |  |  |  |

## Log
## A. soma-cusip engine

- `17:50:31` ⚠ soma status: NoSuchKey: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
## B. repo-deep v2 first self-extend

## C. HFM /hf/v1 extension

- `17:50:31`   ofr-hfm/state.json not written yet (NoSuchKey) -- stfm tick pending
## D. Triparty -- every source

- `17:50:31` TRI sample: REPO-TRIV1_AR_AG-F, REPO-TRIV1_AR_AG-P, REPO-TRIV1_AR_B27-F, REPO-TRIV1_AR_B27-P, REPO-TRIV1_AR_B830-F, REPO-TRIV1_AR_B830-P, REPO-TRIV1_AR_CORD-F, REPO-TRIV1_AR_CORD-P, REPO-TRIV1_AR_G30-F, REPO-TRIV1_AR_G30-P, REPO-TRIV1_AR_LE30-F, REPO-TRIV1_AR_LE30-P
- `17:50:31` ✅ REPO-TRIV1_AR_TOT-P: 2960 obs, 2014-08-22 -> 2026-08-06
- `17:50:31` ✅ REPO-TRIV1_TV_TOT-P: 2960 obs, 2014-08-22 -> 2026-08-06
- `17:50:32` ✅ haircut file on S3: haircuts/tri-party-repo_data_current.xlsx (215434 bytes, office_verified=True)
- `17:50:32` ✅ haircut file on S3: haircuts/tri-party-repo_preNov25_history.xlsx (439002 bytes, office_verified=True)
## E. data.html -- what the page shows right now

