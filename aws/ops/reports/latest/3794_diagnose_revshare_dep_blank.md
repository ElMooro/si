# ops 3794 — why rev share + dependency are blank on the boards

**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-07-24T02:54:12+00:00  

## Data

| all_rows | dependency_available_for_leaderboard_names | dependency_in_ledger | leaderboard | member_rows | revshare_available_for_leaderboard_names | revshare_in_ledger | version |
|---|---|---|---|---|---|---|---|
| 3411 |  |  | 50 | 3411 |  |  | 4.2.1 |
|  | 1 | 154 |  |  |  |  |  |
|  |  |  |  |  | 33 | 1675 |  |

## Log
## Coverage in all_rows (the Full Ledger — should be fine)

- `02:54:12`   revenue_share_pct            1675 / 3411
- `02:54:12`   dependency_pct               154 / 3411
- `02:54:12`   criticality_pctile           3411 / 3411
- `02:54:12`   revenue_share_suppressed     1736 / 3411
- `02:54:12`   criticality_basis            3411 / 3411
- `02:54:12`   revenue_currency             1966 / 3411
- `02:54:12`   revenue_coverage_pct         3411 / 3411
## Coverage on the LEADERBOARD (copied dicts)

- `02:54:12`   revenue_share_pct            0 / 50
- `02:54:12`   dependency_pct               0 / 50
- `02:54:12`   criticality_pctile           50 / 50
- `02:54:12`   revenue_share_suppressed     0 / 50
- `02:54:12`   criticality_basis            0 / 50
- `02:54:12`   revenue_currency             0 / 50
- `02:54:12`   revenue_coverage_pct         0 / 50
## Coverage on INDUSTRY MEMBERS (copied dicts)

- `02:54:12`   revenue_share_pct            0 / 3411
- `02:54:12`   dependency_pct               0 / 3411
- `02:54:12`   criticality_pctile           3411 / 3411
## Is the field even present as a KEY on the copies?

- `02:54:12`   leaderboard[0] has key revenue_share_pct            True
- `02:54:12`   leaderboard[0] has key dependency_pct               True
- `02:54:12`   leaderboard[0] has key criticality_pctile           True
- `02:54:12`   leaderboard[0] has key revenue_share_suppressed     False
- `02:54:12`   leaderboard[0] has key criticality_basis            True
- `02:54:12`   leaderboard[0] has key revenue_currency             False
- `02:54:12`   leaderboard[0] has key revenue_coverage_pct         True
## Sparsity check — is dependency_pct just rare by design?

## VERDICT

- `02:54:12` ⚠ CONFIRMED SNAPSHOT BUG: revenue_share_pct, dependency_pct, revenue_share_suppressed, criticality_basis, revenue_currency, revenue_coverage_pct are 0/50 on the leaderboard but populated in all_rows. The 3790 refresh copies only the six growth fields; the v4.1 percentage fields were never added to that list.
- `02:54:12` FIX: extend the refresh to every field computed after the
- `02:54:12` snapshot — or better, refresh from the source row wholesale
- `02:54:12` so a future field can never be forgotten again.
- `02:54:12` ✅ DIAG.explained :: root cause identified
- `02:54:12` ✅ PASS_ALL — diagnosis complete
