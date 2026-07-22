# ops 3747 — insider-industry-cluster page + field-coverage audit

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-07-22T23:34:58+00:00  

## Data

| failed | page | unrendered | verdict |
|---|---|---|---|
| none | insider-industry-cluster.html | none | PASS_ALL |

## Log
## G1 — page served

- `23:34:58`   served page is CURRENT (len=10824) after 0s
- `23:34:58` PASS G1_page_live — len=10824 missing_markers=[]
## G2 — field coverage (live S3 artifact vs page render paths)

- `23:34:58`   top-level keys: ['attribution', 'coverage', 'degraded', 'generated_at', 'industries', 'lookback_days', 'method', 'n_clusters', 'n_diffuse', 'n_industries', 'source_feed', 'version']
- `23:34:58`   per-row keys:   ['awaiting_base_rate', 'ceo_cfo_companies', 'companies', 'dollar_hhi', 'has_exec_conviction', 'hist_n', 'industry', 'insider_rows_in', 'min_companies', 'min_listed_for_rate', 'min_participation_pct', 'n_companies', 'n_insiders', 'n_listed', 'n_transactions', 'participation_floor_pct', 'participation_pct', 'sector', 'strong_companies', 'thin_universe', 'tickers_unmapped', 'tier', 'top_company', 'top_company_share_pct', 'total_value_usd', 'universe_industries', 'z_vs_own_history']
- `23:34:58` PASS G2_field_coverage — every published key has a render path
## G3 — nav manifest (served)

- `23:34:58` PASS G3_nav — listed under ('Research & Tools', 'Insider Industry Cluster')
## G4 — ladder honesty (no sub-floor PEER, biotech guard)

- `23:34:58`   coverage: {'insider_rows_in': 53, 'tickers_unmapped': 20, 'universe_industries': 150, 'min_companies': 3, 'strong_companies': 4, 'min_listed_for_rate': 8, 'min_participation_pct': 4.0}
- `23:34:58`   Biotechnology: tier=DIFFUSE part=1.86% (regression sentinel)
- `23:34:58` PASS G4_ladder_honesty — sub_floor_peer=[] confirmed_without_base_rate=[]
## VERDICT

- `23:34:58` ✅ PASS_ALL — page live, every field rendered
