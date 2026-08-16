# ops 4755 -- site vacuum v2 (corpus extraction + 1-level crawl)

**Status:** success  
**Duration:** 8.1s  
**Finished:** 2026-08-16T16:37:36+00:00  

## Data

| check | value |
|---|---|
| internal_links_found | 46 |
| pages_in_corpus | 20 |
| bundles_fetched | 10 |
| corpus_bytes | 3505732 |
| data_file_candidates | 7 |
| files_banked_this_run | 4 |
| files_in_manifest_total | 4 |

## Log
- `16:37:28` seed /data/ -> status=200 bytes=30417
- `16:37:28` seed /monitoring-tools/ -> status=200 bytes=45882
- `16:37:34`   cand: https://www.financialresearch.gov/bank-systemic-risk-monitor/data/ofr_bsrm.xlsx
- `16:37:34`   cand: https://www.financialresearch.gov/bank-systemic-risk-monitor/data/ofr_bsrm_international_scores.xlsx
- `16:37:34`   cand: https://www.financialresearch.gov/data/files/Interagency-Data-Inventory-2025.xlsx
- `16:37:34`   cand: https://www.financialresearch.gov/data/financial-instrument-reference-database/data/fird.json
- `16:37:34`   cand: https://www.financialresearch.gov/financial-stress-index/data/fsi.csv
- `16:37:34`   cand: https://www.financialresearch.gov/financial-stress-index/files/FSI_Revision_History_2023-06-27.xlsx
- `16:37:34`   cand: https://www.financialresearch.gov/legal-entity-identifier/data/lei_data.csv
- `16:37:34`   already held: ofr_bsrm.xlsx
- `16:37:34`   already held: ofr_bsrm_international_scores.xlsx
- `16:37:34` ✅   banked data_files_Interagency-Data-Inventory-2025.xlsx (169316 bytes)
- `16:37:34` ✅   banked data_financial-instrument-reference-database_data_fird.json (1183413 bytes)
- `16:37:35`   already held: fsi.csv
- `16:37:35` ✅   banked financial-stress-index_files_FSI_Revision_History_2023-06-27.xlsx (125482 bytes)
- `16:37:35` ✅   banked legal-entity-identifier_data_lei_data.csv (4939 bytes)
