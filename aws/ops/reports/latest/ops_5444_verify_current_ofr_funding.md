# OFR deployed source and warehouse-to-hot verification

**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-09-12T14:14:38+00:00  

## Data

| as_of | code_sha256 | data_unavailable | deploy_run | field | provider | release_commit | series | stale | unit | value |
|---|---|---|---|---|---|---|---|---|---|---|
|  | HWtQbCZyHYnP8xcA3TGuxiR1nt7dGBXfi4eTcqQag3A= |  | 34698517763 |  |  | a31baa50623ace7b13752d1aa3a1ab5dde300b60 |  |  |  |  |
| 2026-09-09 |  | False |  | triparty_rate | ofr |  | REPO-TRI_AR_TOT-P | False | Percent | 3.68 |
| 2026-09-09 |  | False |  | triparty_volume | ofr |  | REPO-TRI_TV_TOT-P | False | USD | 2325530510643.57 |
| 2026-09-10 |  | False |  | dvp_rate | ofr |  | REPO-DVP_AR_TOT-P | False | Percent | 3.63 |
| 2026-09-10 |  | False |  | gcf_rate | ofr |  | REPO-GCF_AR_TOT-P | False | Percent | 3.69 |
| 2026-09-10 |  | False |  | sofr | nyfed |  | SOFR | False | Percent | 3.62 |

## Log
- `14:14:38` ✅ Verified public feed: https://justhodl.ai/data/ofr-funding.json
- `14:14:38` ✅ Verified public feed: https://justhodl-data-proxy.raafouis.workers.dev/data/ofr-funding.json
- `14:14:38` ✅ PASS: five current warehouse observations and provenance verified in S3 and both public routes
