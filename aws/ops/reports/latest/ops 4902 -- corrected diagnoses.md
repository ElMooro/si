# ops 4902 — OECD histogram+probe · MIDAS links · chain duty

**Status:** success  
**Duration:** 400.7s  
**Finished:** 2026-08-19T14:28:49+00:00  

## Data

| advanced | as_of_t0 | as_of_t1 | first | hard_403 | heads | mode | n_complete_t0 | n_complete_t1 | n_found | other | other_detail | res_detail | resolvable_404 | sampled | stage | top | total | unlock_detail | unlockable |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | oecd-histogram | [["HTTPError: HTTP Error 429: Too Many Requests", 972], ["HTTPError: HTTP Error 524: <none>", 9], ["HTTPError: HTTP Error 500: Internal Server Error", 7], ["HTTPError: HTTP Error 404: Not Found", 3]] | 991 |  |  |
|  |  |  |  | 0 |  |  |  |  |  | 18 | DSD_BIMTS@DF_BIMTS_HS2017_4D:500;DSD_BIMTS_6D@DF_BIMTS_HS2017_6D:500;DSD_BTIGE@DF_BTIGE:429;DSD_CPA@DF_CRS_CPA:429;DSD_CRS@DF_CRS:429;DSD_RIOMRKR@DF_RIOMARKERS:429 | DF_SDG_GLC:404/re200;DSD_DASHBOARD@MUNI_CHANGE:404/re200;DSD_FUA_CLIM@DF_FIRES:404/re200 | 3 | 21 | oecd-probe |  |  |  | 0 |
|  |  |  |  |  | {} |  |  |  | 0 |  |  |  |  |  | midas-links |  |  |  |  |
| False | 2026-08-19T12:39:35+00:00 | 2026-08-19T12:39:35+00:00 |  |  |  | backfill | 30 | 30 |  |  |  |  |  |  | deep-duty |  |  |  |  |

## Log
- `14:28:49` VERDICT: PASS_WITH_PENDING · {"oecd_diagnosis": "PASS", "midas_links": "PENDING", "deep_chain_duty": "PENDING"}
- `14:28:49` report written: aws/ops/reports/4902.json
