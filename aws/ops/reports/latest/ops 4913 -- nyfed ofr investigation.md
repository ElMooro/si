# ops 4913 — NY Fed + OFR verdicts & fixes

**Status:** success  
**Duration:** 107.6s  
**Finished:** 2026-08-19T18:39:38+00:00  

## Data

| action | age_min | bsrm_age_min | gaps_gt90min_h | invocations_30h | key | lanes | name | site_age_min | stage |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  | [] | 2 |  |  |  |  | markets-autopsy |
| created |  |  |  |  |  |  | justhodl-nyfed-markets-hourly-s |  | schedule |
| created |  |  |  |  |  |  | justhodl-src-mirror-daily |  | schedule |
|  |  | 0.5 |  |  |  | {"ofr-bsrm": {"ofr_bsrm.xlsx": {"status": "fresh", "bytes": 597243}, "ofr_bsrm_international_scores.xlsx": {"status": "fresh", "bytes": 161171}}, "ofr-site": {"harvested": 0, "fresh": 0}} |  | 0.5 | mirror-run |
|  |  |  |  |  | data/warm/_audit/refresh-orphans.json |  |  |  | audit-artifact |
|  | 1.0 |  |  |  |  |  |  |  | markets-fresh |

## Log
- `18:39:38` VERDICT: PASS · {"markets_scheduler_fixed": "PASS", "src_mirror_live": "PASS", "verdicts_banked": "PASS", "markets_fresh": "PASS"}
- `18:39:38` report written: aws/ops/reports/4913.json
