# === opportunities/asymmetric-equity.json ===

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-05-04T20:10:52+00:00  

## Log
- `20:10:52`   size: 28,540b   modified: 2026-05-04T20:08:08+00:00
- `20:10:52`   top keys: ['as_of', 'v', 'summary', 'cross_pollination', 'cutoffs', 'sector_breakdown', 'top_setups', 'value_traps', 'filter_logic']
- `20:10:52` 
- `20:10:52`     as_of                               = 2026-05-04T20:08:06.347388+00:00
- `20:10:52`     v                                   = 1.1
- `20:10:52`     summary                             = dict (keys: ['n_screener_total', 'n_quality_passed', 'n_setups', 'n_value_traps', 'quality_gate_failures', 'new_this_week', 'dropped_this_week', 'n_with_stacking_signals', 'n_insider_clusters_market_wide', 'n_big_insider_buys_market_wide'])
- `20:10:52`       .n_screener_total               = 503
- `20:10:52`       .n_quality_passed               = 175
- `20:10:52`       .n_setups                       = 94
- `20:10:52`       .n_value_traps                  = 21
- `20:10:52`     cross_pollination                   = dict (keys: ['aaii_pts', 'aaii_signal', 'btc_mvrv', 'onchain_extreme_signals'])
- `20:10:52`       .aaii_pts                       = -5
- `20:10:52`       .aaii_signal                    = aaii_extreme_bullish (spread +50% — contrarian hea
- `20:10:52`     cutoffs                             = dict (keys: ['quality', 'safety', 'value', 'momentum', 'stacked'])
- `20:10:52`       .quality                        = 71.1
- `20:10:52`       .safety                         = 72.6
- `20:10:52`       .value                          = 89.3
- `20:10:52`       .momentum                       = 60.9
- `20:10:52`       .stacked                        = 45.0
- `20:10:52`     sector_breakdown                    = dict (keys: ['Healthcare', 'Basic Materials', 'Technology', 'Energy', 'Consumer Cyclical', 'Communication Services', 'Real Estate', 'Industrials', 'Financial Services', 'Consumer Defensive'])
- `20:10:52`       .Healthcare                     = 12
- `20:10:52`       .Basic Materials                = 6
- `20:10:52`       .Technology                     = 31
- `20:10:52`       .Energy                         = 2
- `20:10:52`       .Consumer Cyclical              = 5
- `20:10:52`       .Communication Services         = 6
- `20:10:52`     top_setups                          = list (n=30)
- `20:10:52`       [0] keys: ['symbol', 'name', 'sector', 'price', 'marketCap', 'peRatio', 'psRatio', 'evEbitda', 'roe', 'operatingMargin', 'netMargin', 'revenueGrowth', 'epsGrowth', 'fcfGrowth', 'debtToEquity', 'currentRatio', 'interestCoverage', 'piotroski', 'beta', 'quality_score', 'safety_score', 'value_score', 'momentum_score', 'stacked_score', 'stacked_signals', 'stacked_raw_pts', 'category', 'dims_passed', 'dims_passed_list', 'composite_score']
- `20:10:52`       [0] sample: {'symbol': 'INCY', 'name': 'Incyte Corporation', 'sector': 'Healthcare', 'price': '97.02', 'marketCap': '19382849640.0', 'peRatio': '13.51', 'psRatio': '3.6155', 'evEbitda': '8.5308', 'roe': '0.29', 'operatingMargin': '0.27', 'netMargin': '0.27', 'revenueGrowth': '0.21', 'epsGrowth': '40.19', 'fcfGrowth': '4.44', 'debtToEquity': '0.0061', 'currentRatio': '3.6816', 'interestCoverage': '615.89', 'piotroski': '9', 'beta': '0.857', 'quality_score': '88.5', 'safety_score': '99.9', 'value_score': '97.6', 'momentum_score': '88.9', 'stacked_score': '45.0', 'stacked_raw_pts': '-5', 'category': 'candidate', 'dims_passed': '5', 'composite_score': '84.0'}
- `20:10:52`     value_traps                         = list (n=15)
- `20:10:52`       [0] keys: ['symbol', 'name', 'sector', 'price', 'marketCap', 'peRatio', 'psRatio', 'piotroski', 'debtToEquity', 'category', 'trap_reason']
- `20:10:52`       [0] sample: {'symbol': 'CHTR', 'name': 'Charter Communications, Inc.', 'sector': 'Communication Services', 'price': '166.01', 'marketCap': '20416739850.0', 'peRatio': '4.22', 'psRatio': '0.3737', 'piotroski': '5', 'debtToEquity': '5.8596', 'category': 'value_trap', 'trap_reason': 'piotroski_low'}
- `20:10:52`     filter_logic                        = dict (keys: ['quality_gate', 'setup_filter', 'stacked_conviction'])
- `20:10:52` 
# === risk/recommendations.json ===

- `20:10:52`   size: 15,995b   modified: 2026-05-04T20:08:10+00:00
- `20:10:52`   top keys: ['as_of', 'v', 'regime', 'regime_strength', 'max_gross_exposure_pct', 'drawdown_status', 'summary', 'constraints_applied', 'clusters', 'sized_recommendations', 'warnings']
- `20:10:52` 
- `20:10:52`     as_of                               = 2026-05-04T20:08:08.437726+00:00
- `20:10:52`     v                                   = 1.0
- `20:10:52`     regime                              = NEUTRAL
- `20:10:52`     regime_strength                     = 56.8
- `20:10:52`     max_gross_exposure_pct              = 75.0
- `20:10:52`     drawdown_status                     = dict (keys: ['current_dd_pct', 'peak_date', 'size_multiplier', 'active_trigger'])
- `20:10:52`       .current_dd_pct                 = -0.2
- `20:10:52`       .peak_date                      = 2026-04-25
- `20:10:52`       .size_multiplier                = 1.0
- `20:10:52`       .active_trigger                 = no trigger
- `20:10:52`     summary                             = dict (keys: ['n_candidate_ideas', 'n_clusters', 'total_recommended_size_pct', 'total_pre_caps_pct'])
- `20:10:52`       .n_candidate_ideas              = 30
- `20:10:52`       .n_clusters                     = 15
- `20:10:52`       .total_recommended_size_pct     = 75.01
- `20:10:52`       .total_pre_caps_pct             = 178.33
- `20:10:52`     constraints_applied                 = dict (keys: ['max_single_position_pct', 'max_cluster_pct', 'max_gross_exposure_pct', 'kelly_fraction'])
- `20:10:52`       .max_single_position_pct        = 8.0
- `20:10:52`       .max_cluster_pct                = 25.0
- `20:10:52`       .max_gross_exposure_pct         = 75.0
- `20:10:52`       .kelly_fraction                 = 0.25
- `20:10:52`     clusters                            = list (n=15)
- `20:10:52`       [0] keys: ['id', 'method', 'members', 'avg_correlation', 'size', 'sector']
- `20:10:52`       [0] sample: {'id': 'sector_technology', 'method': 'sector', 'avg_correlation': '0', 'size': '10', 'sector': 'Technology'}
- `20:10:52`     sized_recommendations               = list (n=30)
- `20:10:52`       [0] keys: ['symbol', 'name', 'sector', 'price', 'source', 'raw_conviction', 'phase2b_composite', 'phase2b_dims', 'kelly_raw', 'quality_weight', 'dd_adjusted', 'cluster', 'after_cluster_cap', 'recommended_size_pct', 'reasoning']
- `20:10:52`       [0] sample: {'symbol': 'MU', 'name': 'Micron Technology, Inc.', 'sector': 'Technology', 'price': '579.09', 'source': 'phase2b', 'raw_conviction': '0.799', 'phase2b_composite': '82.9', 'phase2b_dims': '4', 'kelly_raw': '0.08', 'quality_weight': '1.136', 'dd_adjusted': '0.0909', 'cluster': 'corr_MU', 'after_cluster_cap': '0.0909', 'recommended_size_pct': '3.82', 'reasoning': 'Phase2B 4/4 dims (composite 82.9) | clustered with'}
- `20:10:52`     warnings                            = list (n=1)
- `20:10:52`       [0] keys: ['level', 'message']
- `20:10:52`       [0] sample: {'level': 'medium', 'message': 'Raw signal sum (178%) exceeds 150% — over-signaled'}
- `20:10:52` 
# === data/asymmetric-scorer.json ===

- `20:10:52`   ✗ An error occurred (404) when calling the HeadObject operation: Not Found
- `20:10:52` 
# === data/risk-sizer.json ===

- `20:10:52`   ✗ An error occurred (404) when calling the HeadObject operation: Not Found
- `20:10:52` 
