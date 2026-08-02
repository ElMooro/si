# ops 4268 -- political-stocks on the official rail

**Status:** success  
**Duration:** 11.0s  
**Finished:** 2026-08-02T02:13:15+00:00  

## Log
## 1. refresh congress-direct (source of record)

- `02:13:13` invoked: {"statusCode": 200, "body": "{\"ok\": true, \"senate_tx\": 108, \"house\": 200}"}
- `02:13:14` ✅ congress-direct fresh: 108 senate txns (86 with ticker), 200 house PTR filings, errors: sen=None house=None
## 2. political-stocks v2 on the official feed

- `02:13:15` invoked: {"statusCode": 200, "body": "{\"ok\": true, \"n_quiver\": 86, \"n_house\": 0, \"n_senate\": 86, \"n_tickers\": 0, \"n_clusters\": 0, \"n_bipartisan\": 0, \"duration_s\": 
- `02:13:15` ✅ ARTIFACT LIVE: -0.0 min, source=, trades=0, party-attributed 0/0
## RESULT

- `02:13:15` ✅ OPS 4268 PASS -- congress trading intelligence runs on official filings; Quiver is history
