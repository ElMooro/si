# OFR deployed source and warehouse-to-hot verification

**Status:** success  
**Duration:** 6.2s  
**Finished:** 2026-09-12T14:07:11+00:00  

## Data

| as_of | code_sha256 | data_unavailable | deploy_run | field | release_commit | series | stale | unit | value |
|---|---|---|---|---|---|---|---|---|---|
|  | WtjhtRTAj6JHjsxcpPbZI7iJlyUQP8UX63e2HBQXZz4= |  | 34698152865 |  | a26699d7f92317f01e71c5d2698ebc0d231795ec |  |  |  |  |
| 2026-09-09 |  | False |  | triparty_rate |  | REPO-TRI_AR_TOT-P | False | Percent | 3.68 |
| 2026-09-09 |  | False |  | triparty_volume |  | REPO-TRI_TV_TOT-P | False | USD | 2325530510643.57 |
| 2026-09-10 |  | False |  | dvp_rate |  | REPO-DVP_AR_TOT-P | False | Percent | 3.63 |
| 2026-09-10 |  | False |  | gcf_rate |  | REPO-GCF_AR_TOT-P | False | Percent | 3.69 |
| 2026-08-04 |  | False |  | sofr |  | FNYR-SOFR-A | True | Percent | 3.66 |

## Log
- `14:07:08` nyfed_warehouse_sofr={"key": "data/warm/nyfed/sofr.json.gz", "as_of": null, "latest": [{"date": "2026-09-08", "rate": 3.64}, {"date": "2026-09-09", "rate": 3.64}, {"date": "2026-09-10", "rate": 3.62}]}
- `14:07:11` ✅ Verified public feed: https://justhodl.ai/data/ofr-funding.json
- `14:07:11` ✅ Verified public feed: https://justhodl-data-proxy.raafouis.workers.dev/data/ofr-funding.json
- `14:07:11` ✅ PASS: five warehouse observations and provenance verified in S3 and both public routes
