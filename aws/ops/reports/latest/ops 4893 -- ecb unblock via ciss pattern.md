# ops 4893 — ECB unblock (ciss-stress access pattern)

**Status:** success  
**Duration:** 212.1s  
**Finished:** 2026-08-18T17:24:14+00:00  

## Data

| accept_winner | ciss_present | data_keys | deploy | detail | failures | fn | found | n_dataflows | n_done | n_keys | n_total | negotiation | note | ok | refreshed | sample | series_count | stage | state_present | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | ok |  |  | justhodl-ecb-full-catalog |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | ok |  |  | justhodl-provider-catalog |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | ok |  |  | justhodl-import-sentinel |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| no-accept |  |  |  |  |  |  |  | 104 |  |  |  | [{"attempt": "no-accept", "status": 200}] |  | True |  |  |  | catalog-invoke |  |  |
|  | True |  |  |  |  |  |  | 104 |  |  |  |  |  |  |  | AGR,AME,BKN,BLS,BNT,BOP,BSI,BSP,CAR,CBD,CBD2,CCP |  | catalog-readback |  |  |
|  |  | 2 |  |  | 0 |  |  |  | 2 |  | 104 |  |  |  |  |  |  | walker | True | converging |
|  |  |  |  | converging — 2/104 |  |  | True |  |  |  |  |  |  |  |  |  |  | sentinel |  | RUNNING |
|  |  |  |  |  |  |  |  |  |  | 182 |  |  | ciss-stress engine: 65 CISS/CLIFS/SovCISS stress series live via format=csvdata — the access pattern ops 4893 ported to the catalog builder |  | True |  | 104 | provider-catalog |  |  |

## Log
- `17:20:42`   zip: 100323 bytes
## 1. Lambda

- `17:20:42`   Lambda exists — updating
- `17:20:45` ✅   ✓ updated justhodl-ecb-full-catalog
- `17:20:45`   zip: 108440 bytes
## 1. Lambda

- `17:20:45`   Lambda exists — updating
- `17:20:51` ✅   ✓ updated justhodl-provider-catalog
- `17:20:51`   zip: 103818 bytes
## 1. Lambda

- `17:20:51`   Lambda exists — updating
- `17:20:56` ✅   ✓ updated justhodl-import-sentinel
- `17:20:56` invoking justhodl-ecb-full-catalog (RequestResponse)…
- `17:20:58` walker Event fired (agency=ecb, budget=700); polling state + data keys up to ~6 min…
- `17:21:44` provider-catalog Event fired; polling refresh up to ~6 min (full-bucket scan)…
- `17:24:14` VERDICT: PASS · gates={"deploys": "ok", "ecb_catalog_banked": "PASS", "ecb_walk_started": "PASS", "sentinel_unblocked": "PASS", "ecb_card_series_and_note": "PASS"}
- `17:24:14` report written: aws/ops/reports/4893.json
