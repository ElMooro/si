
# 1) Load actual data/nobrainers.json schema

- `17:28:47`     top-level keys: ['all_scored', 'duration_s', 'generated_at', 'layers_loaded', 'method', 'n_candidates_scored', 'n_unique_tickers', 'schema', 'schema_version', 'summary']
- `17:28:47`     summary keys: ['mu_grade_top_15', 'n_mu_grade', 'n_tier_a_nobrainer', 'n_tier_b_high_conviction', 'n_tier_c_watchlist', 'top_10_tier3', 'top_15_tier2', 'top_25_overall']
- `17:28:47`     candidate keys: ['asymmetric_score', 'factors', 'flag', 'fundamentals', 'name', 'next_earnings', 'supply_signals', 'theme_etf', 'theme_name', 'theme_phase', 'ticker', 'tier', 'valuation_components']
- `17:28:47`       ticker: TX
- `17:28:47`       asymmetric_score: 86.5
- `17:28:47`       theme_etf: SLX
- `17:28:47`       theme_name: Steel
- `17:28:47`       tier: 2
- `17:28:47`       flag: TIER_A_NOBRAINER
- `17:28:47`       factors: {'theme_attribution': 77.5, 'primary_inflated': 50.0, 'supply_inflection': 94.7, 'valuation_asym': 68.8, 'catalyst_prox': 100.0, 'tier_multiplier': 1.0, 'phase_multiplier': 1.1, 'raw_pre_mult': 78.6}

# 2) Load data/nobrainers-rationale.json schema

- `17:28:47`     top-level keys: ['duration_s', 'generated_at', 'layer4_generated_at', 'method', 'min_score_threshold', 'model', 'n_claude_fail', 'n_claude_ok', 'n_layer4_leaderboard', 'n_layer4_mu_grade', 'n_theses', 'schema_version', 'skipped_claude', 'theses']
- `17:28:47`     thesis keys: ['asymmetric_score', 'candidate', 'claude_usage', 'error', 'flag', 'generated_at', 'theme_etf', 'thesis', 'thesis_chars', 'ticker', 'tier']
- `17:28:47`       ticker: TX
- `17:28:47`       thesis: # TX (TERNIUM S.A.) — LONG TIER-2 STEEL PLAY ON SUPPLY INFLECTION

**THE MEGATRE...
- `17:28:47`       asymmetric_score: 86.5
- `17:28:47`       theme_etf: SLX
- `17:28:47`       tier: 2

# 3) Inspect nobrainers.html — what fields does it read?

- `17:28:47`     field refs in JS: ['asymmetric_score', 'candidate', 'classList', 'factors', 'flag', 'fundamentals', 'innerHTML', 'name', 'next_earnings', 'supply_signals', 'theme_etf', 'theme_phase', 'thesis', 'ticker', 'tier']
- `17:28:47`     fundamentals refs: []
- `17:28:47`     factor refs: []

# 4) Live curl — confirm both pages return 200 + nobrainers.html actually renders

- `17:28:47`     200       20546b  https://justhodl.ai/nobrainers.html
- `17:28:47`     200       15362b  https://justhodl.ai/themes.html
- `17:28:47`     200      456897b  https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/nobrainers.json
- `17:28:47`     200       52462b  https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/nobrainers-rationale.json
- `17:28:47`     200       57869b  https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/themes-detected.json
- `17:28:48`     200       68210b  https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/supply-inflection.json
- `17:28:48`     200      306322b  https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/theme-tiers.json

# 5) Cross-reference: are pages reachable from canonical sidebar?

- `17:28:48`     index.html                themes_link=False  nobrainers_link=False
- `17:28:48`     desk.html                 themes_link=False  nobrainers_link=False
- `17:28:48`     brief.html                themes_link=True  nobrainers_link=True
- `17:28:48`     calls.html                themes_link=True  nobrainers_link=True
- `17:28:48`     performance.html          themes_link=True  nobrainers_link=True