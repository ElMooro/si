# ops 4898 — 104 → 214, census-clean

**Status:** success  
**Duration:** 566.0s  
**Finished:** 2026-08-18T19:43:41+00:00  

## Data

| accept_winner | agencies | as_of | banked | catalog_v2 | deep_complete | deep_mode | extras | extras_n | fail_sample | live | n_dataflows | n_deep | n_deep_complete | n_done | n_failures | n_fast | n_total | n_truncated | note | portal | series_count | stage | status | walker_fr |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | settle |  | True |
| no-accept | {"ECB": 104, "ECB.DISS": 89, "ESTAT": 11, "EUROSTAT": 6, "IMF": 4} |  |  |  |  |  |  |  |  |  | 214 |  |  |  |  |  |  |  |  |  |  | catalog-v2 |  |  |
|  |  |  |  |  |  |  |  |  | {"DD": "HTTPError: HTTP Error 404: Not Found", "ECB.DISS:JVC_PUB": "HTTPError: HTTP Error 404: Not Found", "ECB.DISS:LCI_PUB": "HTTPError: HTTP Error 404: Not Found", "ECB.DISS:MOBILE_KEY_4": "HTTPError: HTTP Error 404: Not Found", "ECB.DISS:MOBILE_KEY_5": "HT |  |  |  |  | 214 | 7 |  | 214 | 48 |  |  |  | walk | COMPLETE |  |
|  |  |  | 214 |  |  |  |  | 0 |  |  |  |  |  |  |  |  |  |  |  | 214 |  | census-recheck |  |  |
|  |  | 2026-08-18T19:40:31+00:00 |  |  | 13 | backfill |  |  |  | True |  | 31 | 13 |  |  | 73 |  |  |  |  |  | coverage |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ciss-stress engine: 65 CISS/CLIFS/SovCISS stress series live via format=csvdata — the access pattern ops 4893 ported to the catalog builder · walk+deep coverage: 73 fast + 31 deep-sliced flows · 13/31 deep complete |  | 214 | ecb-card |  |  |

## Log
- `19:34:39` blitz 1: done 104/104
- `19:43:41` VERDICT: PASS · {"patches_deployed": "PASS", "catalog_214": "PASS", "walk_214_complete": "PASS", "census_zero_extras": "PASS", "coverage_ledger_live": "PASS", "ecb_card_coverage_note": "PASS"}
- `19:43:41` report written: aws/ops/reports/4898.json
