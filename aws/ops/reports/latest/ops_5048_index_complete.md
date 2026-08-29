## P0/P1 ecb -- inputs and monotonicity

**Status:** success  
**Duration:** 5.2s  
**Finished:** 2026-08-29T22:33:18+00:00  

## Data

| ecb | eurostat |
|---|---|
| 207/207 | 8147/8147 |

## Log
- `22:33:13`   page->flow entries=6,481  flows_done=207  n_pages=6,481
- `22:33:13`   anchored flows (open at least one page): 118 of 207 (43.0% were invisible to ops 5047)
- `22:33:13`   monotonicity violations: 0 []
## P2 ecb -- interpolate every flow

- `22:33:13`   flows with a range: 207 / 207   missing: 0 []
## P3 ecb -- prove the previously invisible flows

- `22:33:13`   interpolated (never opened a page): 89
- `22:33:13`   AME                FOUND on page-0000 (that page carries 3 flows)
- `22:33:14`   BNT                not on lo..lo+1 (range 3..49 spans 47 pages -- superset, still valid)
- `22:33:14`   BP6_PUB            not on lo..lo+1 (range 70..78 spans 9 pages -- superset, still valid)
- `22:33:14`   BPS_PUB            not on lo..lo+1 (range 78..84 spans 7 pages -- superset, still valid)
- `22:33:14`   sampled 4 interpolated flows
## P4 ecb -- publish

- `22:33:14`   -> index/ecb/flows.json.gz  0.00 MB gz  flows=207 (anchored 118 + interpolated 89)
## P0/P1 eurostat -- inputs and monotonicity

- `22:33:16`   page->flow entries=1,128,408  flows_done=8147  n_pages=1,128,408
- `22:33:16`   anchored flows (open at least one page): 6326 of 8147 (22.4% were invisible to ops 5047)
- `22:33:16`   monotonicity violations: 0 []
## P2 eurostat -- interpolate every flow

- `22:33:16`   flows with a range: 8147 / 8147   missing: 0 []
## P3 eurostat -- prove the previously invisible flows

- `22:33:16`   interpolated (never opened a page): 1821
- `22:33:16`   AACT_ALI01_R       FOUND on page-0000 (that page carries 3 flows)
- `22:33:16`   AEI_HRI            FOUND on page-2202 (that page carries 4 flows)
- `22:33:17`   AEI_PESTSAL_RSK    FOUND on page-2202 (that page carries 4 flows)
- `22:33:17`   APRI_AIP_EN        FOUND on page-2892 (that page carries 3 flows)
- `22:33:17`   sampled 4 interpolated flows
## P4 eurostat -- publish

- `22:33:17`   -> index/eurostat/flows.json.gz  0.09 MB gz  flows=8147 (anchored 6326 + interpolated 1821)
## state-bloat fix landed?

- `22:33:17`   ecb       state 0.8 MB · page_hashes=6,481 (was 98.1 MB / 1,124,942 for eurostat)
- `22:33:17`   eurostat  state 5.5 MB · page_hashes=60,000 (was 98.1 MB / 1,124,942 for eurostat)
- `22:33:18`   -> data/ops/index-tier0.json
- `22:33:18` ops 5048 GREEN -- every flow indexed, premise checked not assumed
