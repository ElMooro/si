# ops 3802 — % notation + dependency labelling

**Status:** success  
**Duration:** 17.1s  
**Finished:** 2026-07-24T15:36:08+00:00  

## Data

| dependency_published_ledger | exactly_100 | graph_nodes | invoke_seconds | invoke_status | leaderboard | leaderboard_with_reason_or_value | rows | version |
|---|---|---|---|---|---|---|---|---|
|  |  |  | 14.6 | 200 |  |  |  |  |
|  |  |  |  |  | 50 |  | 1269 | 4.3.2 |
|  |  |  |  |  | 50 | 50 |  |  |
| 156 | 0 | 183 |  |  |  |  |  |  |

## Log
## Engine settle (v4.3.2)

- `15:35:52` ✅ v4.3.2 artifact live (attempt 1)
- `15:35:52` ✅ DEPLOY.settled :: % notation present in deployed zip
## No 'pp' left in engine-authored text

- `15:36:08` ✅ ENGINE.no_pp :: method + legs_why carry no 'pp'
## Dependency: every leaderboard row must carry a REASON

- `15:36:08` ✅ DEP.reason_present :: 50 of 50 rows explain themselves
- `15:36:08` ✅ DEP.no_fake_100 :: 0 rows print >=99.9%
## Served pages

- `15:36:08` ✅ PAGE.stamp :: capture-gap v10 served (44906 bytes)
- `15:36:08` ✅ PAGE.no_pp :: no 'pp' rendered on capture-gap
- `15:36:08` ✅ PAGE.unmapped_label :: dependency shows 'unmapped'
- `15:36:08` ✅ PAGE.thin_label :: dependency shows 'thin' for peer-floor cases
- `15:36:08` ✅ PAGE.rank_caveat :: glossary states capture gap is percent of RANK, not price
- `15:36:08` ✅ WHY.no_pp :: why.html tiles carry no 'pp'
## Sample — leaderboard dependency now self-explaining

- `15:36:08`   CRUS   dep=0.9%    peers=41   reason=
- `15:36:08`   AOS    dep=5.3%    peers=13   reason=
- `15:36:08`   NVEC   dep=—       peers=41   reason=no mapped supplier links for this company
- `15:36:08`   PEGA   dep=—       peers=3    reason=no mapped supplier links for this company
- `15:36:08`   GDDY   dep=—       peers=8    reason=no mapped supplier links for this company
- `15:36:08`   FDS    dep=—       peers=0    reason=no mapped supplier links for this company
- `15:36:08`   QLYS   dep=—       peers=8    reason=no mapped supplier links for this company
- `15:36:08`   MORN   dep=—       peers=0    reason=no mapped supplier links for this company
## VERDICT

- `15:36:08` ✅ PASS_ALL — % everywhere; dependency blanks now say why
