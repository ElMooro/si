# ops 3782 — verify v5 sorting controls served

**Status:** success  
**Duration:** 125.3s  
**Finished:** 2026-07-23T23:17:38+00:00  

## Data

| pos_by_industry | pos_full_ledger | pos_leaderboard |
|---|---|---|
| 4364 | 5477 | 4253 |

## Log
- `23:15:33` attempt 1: HTTP 200 · 28187 bytes · 0/7 markers
- `23:15:58` attempt 2: HTTP 200 · 28187 bytes · 0/7 markers
- `23:16:23` attempt 3: HTTP 200 · 28187 bytes · 0/7 markers
- `23:16:48` attempt 4: HTTP 200 · 28187 bytes · 0/7 markers
- `23:17:13` attempt 5: HTTP 200 · 28187 bytes · 0/7 markers
- `23:17:38` attempt 6: HTTP 200 · 30839 bytes · 7/7 markers
## v5 controls

- `23:17:38` ✅ SERVED.version_stamp :: present
- `23:17:38` ✅ SERVED.leader_sort_attr :: present
- `23:17:38` ✅ SERVED.industry_sort_attr :: present
- `23:17:38` ✅ SERVED.render_leader :: present
- `23:17:38` ✅ SERVED.render_byind :: present
- `23:17:38` ✅ SERVED.industry_filter :: present
- `23:17:38` ✅ SERVED.asc_desc_hint :: present
## Additive — v4 sections and blend note must survive

- `23:17:38` ✅ KEPT.Most_Undervalued :: intact
- `23:17:38` ✅ KEPT.By_Industry :: intact
- `23:17:38` ✅ KEPT.Structurally_Undervalued :: intact
- `23:17:38` ✅ KEPT.Hidden_Capture_Gaps :: intact
- `23:17:38` ✅ KEPT.Creation_vs_Capture :: intact
- `23:17:38` ✅ KEPT.Full_Ledger :: intact
- `23:17:38` ✅ KEPT.Cross-Industry_Gap :: intact
- `23:17:38` ✅ KEPT.Under-Capitalised_Industri :: intact
- `23:17:38` ✅ KEPT.top_undervalued_all_indust :: intact
- `23:17:38` ✅ KEPT.by_industry :: intact
- `23:17:38` ✅ KEPT.catchup_pct :: intact
- `23:17:38` ✅ KEPT.Default_rank_is_blended :: intact
## Leaderboard is still the FIRST board on the page

- `23:17:38` ✅ ORDER.leaderboard_first :: Most Undervalued precedes By Industry and Full Ledger
## VERDICT

- `23:17:38` ✅ PASS_ALL — sortable leaderboard + industry filter + sortable industry board live
