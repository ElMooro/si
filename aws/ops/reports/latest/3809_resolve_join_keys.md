# ops 3809 — resolve exact join keys

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-07-24T17:24:19+00:00  

## Data

| boom_industries | boom_key | industry_overlap | ledger_industries |
|---|---|---|---|
| 119 | industry | 119 | 141 |

## Log
## dark-pool (912 overlap) — which container holds all names?

- `17:24:18`   list 'board' n=60 keys=['ats_shares_wk', 'conviction', 'daily_off_exch_vol', 'daily_short_pct', 'daily_short_z', 'dark_accel', 'dark_pool_pct', 'offex_pct', 'offex_shares_wk', 'score', 'state', 'ticker', 'total_vol_wk', 'week_return_pct']
- `17:24:18`   list 'top_picks' n=20 keys=['dark_accel', 'dark_pool_pct', 'direction', 'offex_pct', 'score', 'state', 'ticker', 'week_return_pct']
- `17:24:18`   list 'top_accumulation' n=20 keys=['ats_shares_wk', 'conviction', 'daily_off_exch_vol', 'daily_short_pct', 'daily_short_z', 'dark_accel', 'dark_pool_pct', 'offex_pct', 'offex_shares_wk', 'score', 'state', 'ticker', 'total_vol_wk', 'week_return_pct']
- `17:24:18`   list 'top_distribution' n=12 keys=['ats_shares_wk', 'dark_accel', 'dark_pool_pct', 'offex_pct', 'offex_shares_wk', 'score', 'state', 'ticker', 'total_vol_wk', 'week_return_pct']
- `17:24:18`   dict 'dark_map' n=939 (keyed by ticker?)
- `17:24:18`   dict 'xray_map' n=939 (keyed by ticker?)
## finra-short (494) — containers

- `17:24:18`   list 'squeeze_candidates' n=20 keys=['days_to_cover', 'momentum_pct', 'name', 'price_strength', 'sector', 'short_volume', 'squeeze_flags', 'squeeze_score', 'svr_pct', 'symbol', 'total_volume', 'z_score']
- `17:24:18`   list 'top_svr' n=30 keys=['name', 'sector', 'short_volume', 'svr_pct', 'symbol', 'total_volume', 'z_score']
- `17:24:18`   list 'top_zscore' n=30 keys=['momentum_pct', 'name', 'sector', 'short_volume', 'svr_pct', 'symbol', 'z_score']
- `17:24:18`   dict 'tickers' n=501
## earnings-pead (241) — containers + metric shape

- `17:24:18`   list 'all_qualifying' n=244 keys=['beat_streak', 'flags', 'metrics', 'score', 'surprise_history', 'symbol', 'tier']
- `17:24:18`     metrics: {"latest_earnings_date": "2026-05-28", "days_since_earnings": 57.0, "drift_active": true, "latest_surprise_pct": 64.2, "avg_surprise_4q": 20.1, "post_earnings_return_pct": 38.5, "market_cap": 29082899
- `17:24:18`     tier=TIER_S_PEAD_DRIFT score=90.0 flags=['BEAT_STREAK_4Q+', 'BIG_BEAT_30%+', 'AVG_SURPRISE_20%+', 'DRIFT_ACTIVE', 'DRIFT
## estimate-revisions (236) — containers + revision field

- `17:24:18`   dict 'direction_map' n=393 sample['RELL']=FLAT
- `17:24:18`   list 'estimate_strength_leaders' n=40 keys=['baseline_date', 'baseline_eps_est', 'company', 'current_eps_est', 'days_to_earnings', 'direction', 'dispersion_pct', 'earnings_date', 'eps_rev_pct', 'eps_rev_recent_pct', 'estimate_strength', 'fiscal_period', 'fiscal_year', 'fwd_eps_growth_pct', 'importance', 'n_analysts']
- `17:24:18`   list 'top_picks' n=15 keys=['days_to_earnings', 'earnings_date', 'eps_rev_pct', 'fwd_eps_growth_pct', 'revenue_confirms', 'score', 'ticker']
## industry-boom — how is the league keyed?

- `17:24:18`   list 'league' n=119 keys=['boom_score', 'comp', 'coverage_w', 'industry', 'mcap_b', 'n', 'n_component_families', 'score_delta_20d', 'sector', 'top_names']
- `17:24:18`     sample: {"industry": "Computer Hardware", "sector": "Technology", "n": 38, "mcap_b": 1373.7, "boom_score": 83.1, "n_component_families": 9, "coverage_w": 115.0, "comp": {"rev_mean": 79.10000000000001, "rev_breadth": 100.0, "deal_wins_30d": 2, "backlog_accel_share": 0.
- `17:24:18`   list 'trouble' n=10 keys=['boom_score', 'comp', 'coverage_w', 'industry', 'mcap_b', 'n', 'n_component_families', 'score_delta_20d', 'sector', 'top_names']
- `17:24:18`   n_industries=119 coverage={'sources_ok': {'universe': True, 'revisions': True, 'deal_ledger': True, 'backlog': True,
## Ledger industry names must MATCH industry-boom league names

- `17:24:19`   ledger-only sample: ['Aluminum', 'Asset Management', 'Auto - Recreational Vehicles', 'Banks', 'Broadcasting', 'Coal']
- `17:24:19`   matched sample    : ['Advertising Agencies', 'Aerospace & Defense', 'Agricultural - Machinery', 'Agricultural Farm Products', 'Agricultural Inputs', 'Airlines, Airports & Air Services']
- `17:24:19` ✅ PASS_ALL — keys resolved
