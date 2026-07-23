# ops 3783 — % critical to industry (revenue share + pctile + dependency)

**Status:** success  
**Duration:** 43.4s  
**Finished:** 2026-07-23T23:24:38+00:00  

## Data

| invoke_seconds | invoke_status | scored | share_max | share_min | with_dependency | with_revenue_share |
|---|---|---|---|---|---|---|
| 21.6 | 200 |  |  |  |  |  |
|  |  | 2322 |  |  | 154 | 1426 |
|  |  |  | 100.0 | 0.0 |  |  |

## Log
## G0 — verify what exists before consuming it

- `23:23:55` ✅ G0.rev_computed :: evaluate() already derives TTM revenue (currently discarded)
- `23:23:55` ✅ G0.rev_not_persisted :: revenue NOT yet a row field — this is the gap
- `23:23:55` ✅ G0.centrality :: supply-chain edge count per symbol in scope
- `23:23:55` ✅ G0.cap_rows :: cap_rows field-carry block present (3771 lesson: must extend it)
- `23:23:55` ✅ G0.v401 :: engine at v4.0.1
## [1] Persist revenue_ttm from evaluate()

- `23:23:55` ✅ P1.anchor :: evaluate return anchor unique
## [2] Carry through cap_rows

- `23:23:55` ✅ P2.anchor :: cap_rows carry anchor unique
## [3] Compute revenue_share_pct / dependency_pct / basis

- `23:23:55` ✅ P3.anchor :: sort anchor unique
- `23:23:55` ✅ P3.post_anchor :: post anchor unique
- `23:23:55` ✅ v4.1 spliced + compile clean
## Deploy

- `23:23:55`   zip: 100542 bytes
## 1. Lambda

- `23:23:56`   Lambda exists — updating
- `23:24:01` ✅   ✓ updated justhodl-chokepoint
- `23:24:16` ✅ settled attempt 1
- `23:24:16` ✅ DEPLOY.settled :: v4.1 live
## Invoke + verify

- `23:24:38` ✅ LIVE.v41 :: version=4.1
- `23:24:38` ✅ LIVE.no_error :: err=None
- `23:24:38` ✅ LIVE.rev_share :: revenue_share_pct on 1426 names
- `23:24:38` ✅ LIVE.note :: interpretation note shipped in feed
- `23:24:38` ✅ SANITY.bounds :: max share 100.00% within bounds
- `23:24:38` ✅ SANITY.sums_to_100 :: max industry share-sum 100.0% (must be ~100 by construction)
## Sample — the three percentages side by side

- `23:24:38`   TSM   crit=71.4  pctile=89.0  rev_share=3.77%   dep=5.9%   basis=business quality (margins/ROIC/R&D)
- `23:24:38`   ASML  crit=73.7  pctile=95.1  rev_share=0.03%   dep=3.0%   basis=business quality (margins/ROIC/R&D)
- `23:24:38`   NVDA  crit=88.2  pctile=98.8  rev_share=0.21%   dep=14.9%  basis=business quality (margins/ROIC/R&D)
- `23:24:38`   AMD   crit=56.4  pctile=67.1  rev_share=0.03%   dep=5.9%   basis=mixed
- `23:24:38`   AVGO  crit=67.3  pctile=81.7  rev_share=0.06%   dep=5.0%   basis=business quality (margins/ROIC/R&D)
- `23:24:38`   MSFT  crit=70.6  pctile=88.3  rev_share=5.81%   dep=—      basis=business quality (margins/ROIC/R&D)
## Additive contract

- `23:24:38` ✅ ADDITIVE.structural_names :: present
- `23:24:38` ✅ ADDITIVE.industry_leaders :: present
- `23:24:38` ✅ ADDITIVE.all_chokepoints :: present
- `23:24:38` ✅ ADDITIVE.row_capture_gap :: prior field preserved
- `23:24:38` ✅ ADDITIVE.row_catchup_pct :: prior field preserved
- `23:24:38` ✅ ADDITIVE.row_global_capture_gap :: prior field preserved
## VERDICT

- `23:24:38` ✅ PASS_ALL — three labelled percentages live; composite never shown as a share
