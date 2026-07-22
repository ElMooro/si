# ops 3732 — import-canary page + field-coverage audit

**Status:** success  
**Duration:** 161.8s  
**Finished:** 2026-07-22T20:46:36+00:00  

## Data

| coverage_pct | failed | page | unrendered | verdict |
|---|---|---|---|---|
| 100.0 | none | import-canary.html | none | PASS_ALL |

## Log
## G1 — page served

- `20:43:55`   attempt 0: HTTP Error 404: Not Found
- `20:44:15`   attempt 1: HTTP Error 404: Not Found
- `20:44:35`   attempt 2: HTTP Error 404: Not Found
- `20:44:55`   attempt 3: HTTP Error 404: Not Found
- `20:45:15`   attempt 4: HTTP Error 404: Not Found
- `20:45:35`   attempt 5: HTTP Error 404: Not Found
- `20:45:55`   attempt 6: HTTP Error 404: Not Found
- `20:46:16`   attempt 7: HTTP Error 404: Not Found
- `20:46:36` PASS G1_page_live — len=12794 missing_markers=[]
## G2 — field coverage (live S3 artifact vs page render paths)

- `20:46:36`   top-level keys: ['attribution', 'coverage', 'data_month', 'degraded', 'generated_at', 'industry_rollup', 'lag_note', 'lines', 'n_lines', 'naics_lines', 'scope_note', 'signals', 'source_url', 'version']
- `20:46:36`   per-row keys:   ['accel_pp', 'all_pp', 'basis', 'code', 'codes', 'concentration', 'covered_usd', 'fragile', 'gainer', 'gainer_pp', 'hhi', 'hist_n', 'import_usd_mo', 'import_yoy_pct', 'industry', 'label', 'level', 'level_code', 'loser', 'loser_pp', 'mom_3m_pct', 'month', 'n_lines', 'n_months', 'shares_pct', 'source_shift', 'tier', 'top_share_pct', 'top_source', 'yoy_pct', 'yoy_prev_pct', 'z_yoy']
- `20:46:36` PASS G2_field_coverage — every published key has a render path
## G3 — nav manifest (served)

- `20:46:36` PASS G3_nav — listed under ('Macro & Liquidity', 'Import Canary')
## G4 — degraded lines

- `20:46:36`   coverage: {'hs_requested': 26, 'hs_ok': 26, 'naics_requested': 7, 'naics_ok': 7}
- `20:46:36` PASS G4_degraded — coverage 100% (33/33), degraded=0
## VERDICT

- `20:46:36` ✅ PASS_ALL — page live, every field rendered
