# ops 4910 — hist-banker · sec-bulk daily · freshness sweep

**Status:** success  
**Duration:** 202.4s  
**Finished:** 2026-08-19T17:05:01+00:00  

## Data

| action | classification | lanes | live | name | newest_age_min | note | provider | stage | still_missing |
|---|---|---|---|---|---|---|---|---|---|
| created |  |  |  | justhodl-hist-banker-weekly |  |  |  | schedule |  |
|  |  |  | True |  |  |  |  | banker |  |
|  |  | {"dera": {"inv": 69, "have": 10, "fails": 0}, "edgar": {"inv": 135, "have": 5, "fails": 0}, "eiopa": {"inv": 67, "have": 5, "fails": 0}} |  |  |  |  |  | banker-first-items | 256 |
| created |  |  |  | justhodl-sec-bulk-daily |  |  |  | schedule |  |
|  |  |  |  |  | 0.5 |  |  | sec-bulk |  |
|  |  |  |  |  | 3966.9 | repo engine daily; source files weekly — age tracks upstream |  | nyfed-research |  |
|  | quarterly-class source |  |  |  | 4359.4 |  | ofr-bsrm | cadence |  |
|  | weekly/quarterly files |  |  |  | 4347.4 |  | ofr-site | cadence |  |
|  | already fresh |  |  |  | 58.4 |  | ofr-hfm | cadence |  |

## Log
- `17:05:01` VERDICT: PASS · {"hist_banker_live_banking": "PASS", "sec_bulk_fresh_daily": "PASS", "nyfed_refreshed": "PASS", "cadence_classified": "PASS"}
- `17:05:01` report written: aws/ops/reports/4910.json
