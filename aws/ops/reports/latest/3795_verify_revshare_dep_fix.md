# ops 3795 — rev share + dependency on the boards (v4.2.2)

**Status:** failure  
**Duration:** 18.1s  
**Finished:** 2026-07-24T03:01:17+00:00  

## Error

```
SystemExit: 1
```

## Data

| dependency_leaderboard | dependency_ledger | dependency_members | invoke_seconds | invoke_status |
|---|---|---|---|---|
|  |  |  | 16.6 | 200 |
| 0 | 153 | 0 |  |  |

## Log
## Zip settle

- `03:01:00` ✅ v4.2.2 artifact live (attempt 1)
- `03:01:00` ✅ DEPLOY.settled :: wholesale refresh present in deployed zip
## Invoke

- `03:01:17` ✅ LIVE.v422 :: version=4.2.2
## THE FIX — copies must now carry the percentage fields

- `03:01:17` ✗ LEAD.revenue_share_pct :: 0 of 50 leaderboard rows (was 0/50)
- `03:01:17` ✅ LEAD.criticality_pctile :: 50 of 50 leaderboard rows (was 0/50)
- `03:01:17` ✗ LEAD.criticality_basis :: 0 of 50 leaderboard rows (was 0/50)
- `03:01:17` ✗ MEMBERS.revenue_share_pct :: 0 of 3012 member rows (was 0)
- `03:01:17` ✅ MEMBERS.criticality_pctile :: 3012 of 3012 member rows (was 0)
## Dependency — expected to stay sparse (graph coverage)

- `03:01:17` ⚠ 0 leaderboard names have mapped supplier links — the curated graph does not cover the small/micro caps that dominate the board. Honest sparsity, now labelled on the page.
- `03:01:17` ✗ DEP.members_reached :: 0 member rows carry dependency (ledger has 153)
## Sample

- `03:01:17`   HERE   rev_share=—         dep=—        crit%ile=94.7   basis=
- `03:01:17`   DOMO   rev_share=—         dep=—        crit%ile=95.3   basis=
- `03:01:17`   LDI    rev_share=—         dep=—        crit%ile=90.9   basis=
- `03:01:17`   BMBL   rev_share=—         dep=—        crit%ile=75.7   basis=
- `03:01:17`   QTTB   rev_share=—         dep=—        crit%ile=93.4   basis=
- `03:01:17`   TREE   rev_share=—         dep=—        crit%ile=90.2   basis=
- `03:01:17`   RCMT   rev_share=—         dep=—        crit%ile=81.8   basis=
- `03:01:17`   CGEN   rev_share=—         dep=—        crit%ile=90.7   basis=
- `03:01:17`   HLF    rev_share=—         dep=—        crit%ile=96.9   basis=
- `03:01:17`   RC     rev_share=—         dep=—        crit%ile=75.0   basis=
## Served page v9

- `03:01:17` attempt 1: 44551 bytes · 4/4
- `03:01:17` ✅ SERVED.stamp :: present
- `03:01:17` ✅ SERVED.dep_tooltip :: present
- `03:01:17` ✅ SERVED.coverage_note :: present
- `03:01:17` ✅ SERVED.rsh :: present
## Additive

- `03:01:17` ✅ ADDITIVE.capture_gap :: leaderboard still carries it
- `03:01:17` ✅ ADDITIVE.catchup_pct :: leaderboard still carries it
- `03:01:17` ✅ ADDITIVE.growth_tier :: leaderboard still carries it
- `03:01:17` ✅ ADDITIVE.in_sp500 :: leaderboard still carries it
## VERDICT

- `03:01:17` ✗ FAILED: LEAD.revenue_share_pct, LEAD.criticality_basis, MEMBERS.revenue_share_pct, DEP.members_reached
