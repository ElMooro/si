# ops 3768 — backlog leg: diagnose, then repair

**Status:** success  
**Duration:** 35.2s  
**Finished:** 2026-07-23T17:51:56+00:00  

## Data

| backlog_by_ticker | backlog_covered | backlog_generated | backlog_joined | backlog_ledger_size | backlog_n_covered | backlog_overlap | chokepoint_generated | chokepoint_scored | invoke_seconds | invoke_status | normalized_overlap | overlap_accelerating | overlap_with_rpo_value | raw_overlap | scored | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 121 |  | 2026-07-23T11:30:46.885287+00:00 |  | 121 | 93 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 2026-07-23T17:41:06.091259+00:00 | 879 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 61 |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 61 |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 43 | 48 |  |  |  |
|  |  |  |  |  |  |  |  |  | 14.3 | 200 |  |  |  |  |  |  |
|  | 61 |  | 49 | 121 |  | 61 |  |  |  |  |  |  |  |  | 879 | 3.2 |

## Log
## Diagnosis — do these two engines share tickers at all?

- `17:51:21`   overlap sample: ['ACIW', 'AMAT', 'AMD', 'AMZN', 'AVAV', 'AVGO', 'BA', 'BKR', 'CACI', 'CAT', 'CMI', 'CRM', 'CRWD', 'CW', 'ETN', 'FROG', 'FTNT', 'GD', 'GDDY', 'GE', 'GNRC', 'GOOGL', 'HAL', 'HEI', 'HII']
- `17:51:21` ✅ DIAG.no_casing_bug :: raw=61 normalized=61 (equal => no casing/whitespace bug)
- `17:51:21` ✅ DIAG.backlog_populated :: backlog ledger has 121 tickers
- `17:51:21`   sample backlog row keys: ['cap_bucket', 'cik', 'deferred_accelerating', 'deferred_asof', 'deferred_filed', 'deferred_qoq', 'deferred_rev', 'deferred_yoy', 'demand_accelerating', 'ev_to_rpo', 'group', 'refreshed_at', 'rev_yoy', 'rpo', 'rpo_asof', 'rpo_filed', 'rpo_form', 'rpo_minus_rev_growth', 'rpo_qoq', 'rpo_tag', 'rpo_yoy', 'sector', 'ticker']
## Cause

- `17:51:21` ✅ populations DO intersect (61 names) — join should yield >0
## Repair — honest coverage accounting

- `17:51:21` ✅ REPAIR.anchor :: join anchor unique
- `17:51:21` ✅ REPAIR.stats_anchor :: stats anchor unique ("backlog_joined": sum(1 for c in cap_row)
- `17:51:21` ✅ repair spliced + compile clean (v3.2)
## Deploy

- `17:51:21`   zip: 95740 bytes
## 1. Lambda

- `17:51:21`   Lambda exists — updating
- `17:51:26` ✅   ✓ updated justhodl-chokepoint
- `17:51:41` ✅ settled attempt 1
- `17:51:41` ✅ DEPLOY.settled :: v3.2 live
## Live coverage — honest numbers

- `17:51:56` ✅ LIVE.v32 :: version=3.2
- `17:51:56` ✅ LIVE.coverage_reported :: overlap now reported in feed (was invisible)
- `17:51:56` ✅ LIVE.additive :: books intact
- `17:51:56` ✅ FIX.leg_alive :: joined=49 of overlap=61
## VERDICT

- `17:51:56` ✅ PASS_ALL — backlog coverage now honest and visible
