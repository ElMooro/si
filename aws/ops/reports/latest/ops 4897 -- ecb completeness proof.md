# ops 4897 — every flow, all history: the proof

**Status:** success  
**Duration:** 1076.8s  
**Finished:** 2026-08-18T19:30:27+00:00  

## Data

| agencies | alive | alive_n | banked | deep_v11 | extras | extras_n | ledger | live | mode | n_complete | n_deep | n_deep_complete | n_fast | n_flows | n_keys | n_total | note | portal_total | provcat | series_count | source_empty_404 | stage |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  | settle |
| {"ECB": 104, "ECB.DISS": 89, "ESTAT": 11, "EUROSTAT": 6, "IMF": 4} |  |  | 104 |  | ECB.DISS:BKN_PUB,ECB.DISS:BP6_PUB,ECB.DISS:BPS_PUB,ECB.DISS:BSI_PUB,ECB.DISS:CPP_PUB,ECB.DISS:CSEC_PUB,ECB.DISS:EDP_PUB,ECB.DISS:ENA_PUB,ECB.DISS:EXR_PUB,ECB.DISS:FM_PUB,ECB.DISS:FVC_PUB,ECB.DISS:GFS_PUB,ECB.DISS:ICB_PUB,ECB.DISS:ICO_PUB,ECB.DISS:ICPF_PUB | 110 |  |  |  |  |  |  |  |  |  |  |  | 214 |  |  |  | dataflow-census |
|  |  | 0 |  |  |  |  | 1 |  |  |  |  |  |  |  |  |  |  |  |  |  | DD | failures-probe |
|  |  |  |  |  |  |  |  | False |  |  | None | None | None |  |  | 0 |  |  |  |  |  | coverage |
|  |  |  |  |  |  |  |  |  | backfill | 13 |  |  |  | 31 |  |  |  |  |  |  |  | deep-progress |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 401 |  | ciss-stress engine: 65 CISS/CLIFS/SovCISS stress series live via format=csvdata — the access pattern ops 4893 ported to the catalog builder |  |  | 104 |  | ecb-card |

## Log
- `19:30:27` VERDICT: FAIL · {"patches_deployed": "PASS", "dataflow_census": "PENDING", "failures_classified": "PASS", "coverage_ledger_live": "FAIL", "ecb_card_coverage_note": "FAIL"}
- `19:30:27` report written: aws/ops/reports/4897.json
