# ops 4083 — real macro attribution (FRED metadata)

**Status:** success  
**Duration:** 248.9s  
**Finished:** 2026-07-29T06:23:11+00:00  

## Data

| agency_rows | attributed | calls | coverage | economics_symbols | macro | macro_attributed | macro_unattributed | unattributed |
|---|---|---|---|---|---|---|---|---|
|  | 239 | 320 | 5.9 |  | 4082 |  |  | 3843 |
| 239 |  |  |  | 239 |  | 239 | 3843 |  |

## Log
## A. deploy justhodl-macro-attribution

- `06:19:03`   ✓ updated justhodl-macro-attribution
- `06:19:14`   ✓ justhodl-macro-attribution marker settled (attempt 2)
## B. invoke — first ledger pass

- `06:22:57`   status=200 fnerr=None
- `06:22:57`   {"statusCode": 200, "body": "{\"attributed\": 239, \"unattributed\": 3843, \"resolved_this_run\": 239}"}
## Publishers resolved (real institutions)

- `06:22:57`      83  Board of Governors of the Federal Reserve System (US)  [FRED]
- `06:22:57`      38  Ice Data Indices, LLC  [OTHER-OFFICIAL]
- `06:22:57`      23  U.S. Bureau of Economic Analysis  [BEA]
- `06:22:57`      14  U.S. Census Bureau  [CENSUS-US]
- `06:22:57`      13  U.S. Bureau of Labor Statistics  [BLS]
- `06:22:57`      10  Organization for Economic Co-operation and Development  [OECD]
- `06:22:57`       6  World Bank  [WORLD-BANK]
- `06:22:57`       5  Federal Reserve Bank of St. Louis  [FRED]
- `06:22:57`       4  Eurostat  [EUROSTAT]
- `06:22:57`       4  U.S. Department of the Treasury. Fiscal Service  [US-TREASURY]
- `06:22:57`       3  U.S. Employment and Training Administration  [OTHER-OFFICIAL]
- `06:22:57`       3  Federal Reserve Bank of Chicago  [FRED]
- `06:22:57`       3  U.S. Office of Management and Budget  [OTHER-OFFICIAL]
- `06:22:57`       2  BCB-BRAZIL  [OTHER-OFFICIAL]
- `06:22:57`   by_route: {'vault-gov': 5, 'vault-fred': 6, 'fred-metadata': 228}
## C. source-map v2.1 merge

- `06:22:58`   ✓ updated justhodl-source-map
- `06:23:09`   ✓ justhodl-source-map marker settled (attempt 2)
- `06:23:10`   {"statusCode": 200, "body": "{\"symbols_with_source\": 197, \"agency_rows\": 239, \"economics_symbols\": 0}"}
- `06:23:10`   agency_families: {'BCB-BRAZIL': 2, 'OECD': 10, 'ECB': 2, 'NORGES': 1, 'BCRP-PERU': 2, 'FRED': 97, 'OTHER-OFFICIAL': 56, 'BEA': 23, 'NBER': 1, 'CENSUS-US': 14, 'BTS-US': 1, 'BLS': 13, 'BOE': 1, 'EUROSTAT': 4, 'IMF': 2, 'WORLD-BANK': 6, 'US-TREASURY': 4}
## D. schedule

- `06:23:11`   ✓ created macro-attribution-daily
- `06:23:11`   state=ENABLED expr=cron(5 12 * * ? *)
## E. field coverage

- `06:23:11`   ✓ agency_families
- `06:23:11`   ✓ agency_rows
- `06:23:11`   ✓ distinct_sources
- `06:23:11`   ✓ economics_agencies
- `06:23:11`   ✓ economics_symbols
- `06:23:11`   · generated_at
- `06:23:11`   ✓ harvest_progress
- `06:23:11`   ✓ junk_purged
- `06:23:11`   ✓ known_families
- `06:23:11`   ✓ macro_attributed
- `06:23:11`   ✓ macro_coverage_pct
- `06:23:11`   ✓ macro_unattributed
- `06:23:11`   · marker
- `06:23:11`   ✓ new_sources
- `06:23:11`   ✓ symbols_with_source
- `06:23:11`   ✓ venue_rows
## VERDICT

- `06:23:11`   ✓ macro-attribution settled
- `06:23:11`   ✓ invoke clean
- `06:23:11`   ✓ attribution is non-empty
- `06:23:11`   ✓ the unattributed gap is PUBLISHED, not hidden
- `06:23:11`   ✓ no attribution invented for inference-only symbols
- `06:23:11`   ✓ every resolved row names its route
- `06:23:11`   ✓ source-map v2.1 settled
- `06:23:11`   ✓ agency_rows is finally NON-ZERO
- `06:23:11`   ✓ macro gap surfaced on the artifact
- `06:23:11`   ✓ schedule ENABLED (verified)
- `06:23:11`   ✓ every source-map key rendered (16/16)
- `06:23:11` ✅ PASS_ALL — 239 macro symbols carry a REAL publisher; 3843 reported honestly unattributed. Ledger accretes daily.
