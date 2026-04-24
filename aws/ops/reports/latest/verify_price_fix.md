# Verify price-fetch fix worked — outcomes actually scoring

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-04-24T23:21:33+00:00  

## Log
## A. Outcome-checker recent invocations + errors

- `23:21:33`   Last 30 min: 2 invocations, 0 errors
- `23:21:33`   Max duration: 115425ms
## B. Sample log output — are prices being fetched?

- `23:21:33`   Latest stream: 2026/04/24/[$LATEST]a680555be7b64d078b5d3030698e91a7 (3.9 min old)
- `23:21:33`   Log line counts (last 30 min):
- `23:21:33`     Scored CORRECT: 0
- `23:21:33`     Scored WRONG:   497
- `23:21:33`     No price:       0
- `23:21:33`     Polygon 403s:   0
- `23:21:33`     FMP 403s:       0
- `23:21:33` 
- `23:21:33` ✅   497 predictions scored, 100% of attempts succeeded
- `23:21:33` 
  Sample CORRECT lines:
- `23:21:33` 
  Sample WRONG lines:
- `23:21:33`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `23:21:33`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `23:21:33`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
## C. Outcomes table item count

- `23:21:33`   Items now:    738
- `23:21:33`   Items before: 738 (per Step 54 baseline)
- `23:21:33`   Net growth:   +0
- `23:21:33` ⚠   No growth — backfill may have failed silently
- `23:21:33`   Size:         304,525 bytes
## D. Sample 5 most-recent outcomes — correct=True/False?

- `23:21:33`   Most recent 10 outcomes (by checked_at):
- `23:21:33`     [2026-04-24T23:17:41] momentum_tlt/day_1: predicted=DOWN, actual=UNKNOWN, return=?, correct=None
- `23:21:33`     [2026-04-24T23:17:41] khalid_index/day_14: predicted=NEUTRAL, actual=UNKNOWN, return=?, correct=None
- `23:21:33`     [2026-04-24T23:17:41] khalid_index/day_14: predicted=NEUTRAL, actual=UNKNOWN, return=?, correct=None
- `23:21:33`     [2026-04-24T23:17:41] ml_risk/day_14: predicted=UP, actual=UNKNOWN, return=?, correct=None
- `23:21:33`     [2026-04-24T23:17:41] khalid_index/day_7: predicted=NEUTRAL, actual=UNKNOWN, return=?, correct=None
- `23:21:33`     [2026-04-24T23:17:41] plumbing_stress/day_14: predicted=UP, actual=UNKNOWN, return=?, correct=None
- `23:21:33`     [2026-04-24T23:17:41] ml_risk/day_7: predicted=UP, actual=UNKNOWN, return=?, correct=None
- `23:21:33`     [2026-04-24T23:17:41] plumbing_stress/day_30: predicted=UP, actual=UNKNOWN, return=?, correct=None
- `23:21:33`     [2026-04-24T23:17:41] momentum_gld/day_3: predicted=UP, actual=UNKNOWN, return=?, correct=None
- `23:21:33`     [2026-04-24T23:17:41] edge_composite/day_7: predicted=NEUTRAL, actual=UNKNOWN, return=?, correct=None
- `23:21:33` 
  In sample of 200:
- `23:21:33`     correct=True:  0
- `23:21:33`     correct=False: 0
- `23:21:33`     correct=None:  200
- `23:21:33` Done
