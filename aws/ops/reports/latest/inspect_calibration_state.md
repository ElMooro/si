# Inspect calibration state (weights + outcomes + signals)

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-05-04T13:08:29+00:00  

## Log
- `13:08:28` ✅   ✓ /justhodl/calibration/weights  len=794
- `13:08:28`     parsed keys: ['crypto_risk_score', 'plumbing_stress', 'crypto_fear_greed', 'momentum_uso', 'edge_composite', 'corr_break_top_pair', 'crisis_hy_oas_vs_hyg', 'corr_break_composite_vs_spy', 'corr_break_composite_vs_vxx', 'crisis_sofr_iorb']
- `13:08:28`       crypto_risk_score: 0.3961
- `13:08:28`       plumbing_stress: 0.5429
- `13:08:28`       crypto_fear_greed: 0.4981
- `13:08:28`       momentum_uso: 0.3816
- `13:08:28`       edge_composite: 0.8317
- `13:08:28` ✅   ✓ /justhodl/calibration/accuracy  len=1156
- `13:08:28`     parsed keys: ['crypto_risk_score', 'plumbing_stress', 'crypto_fear_greed', 'momentum_uso', 'edge_composite', 'corr_break_top_pair', 'crisis_hy_oas_vs_hyg', 'corr_break_composite_vs_spy', 'corr_break_composite_vs_vxx', 'crisis_sofr_iorb']
- `13:08:28`       crypto_risk_score: {'accuracy': 0.2949, 'n': 78, 'avg_return': -0.0207}
- `13:08:28`       plumbing_stress: {'accuracy': 0.4286, 'n': 49, 'avg_return': 0.4586}
- `13:08:28`       crypto_fear_greed: {'accuracy': 0.3974, 'n': 78, 'avg_return': -0.0207}
- `13:08:28`       momentum_uso: {'accuracy': 0.2727, 'n': 55, 'avg_return': 4.3369}
- `13:08:28`       edge_composite: {'accuracy': 0.5714, 'n': 49, 'avg_return': 0.4586}
- `13:08:28` ✅   ✓ justhodl-signals: items≈6339 sizeBytes=4253742
- `13:08:28` ✅   ✓ justhodl-outcomes: items≈5712 sizeBytes=2530080
- `13:08:28` ✅   ✓ justhodl-feedback: items≈0 sizeBytes=0
- `13:08:28`   outcomes sample (n=20):
- `13:08:28`     khalid_index                   n=3
- `13:08:28`     crypto_risk_score              n=3
- `13:08:28`     plumbing_stress                n=3
- `13:08:28`     crypto_fear_greed              n=2
- `13:08:28`     edge_composite                 n=2
- `13:08:28`     screener_top_pick              n=2
- `13:08:28`     ml_risk                        n=2
- `13:08:28`     momentum_tlt                   n=1
- `13:08:28`     momentum_gld                   n=1
- `13:08:28`     market_phase                   n=1
- `13:08:28`   paper portfolio: keys=['version', 'generated_at', 'as_of_date', 'first_seen', 'last_run_date', 'initial_nav', 'current_nav', 'current_nav_pct_chg', 'unrealized_pnl_dollars', 'open_positions']
- `13:08:28`   ab-test: keys=['as_of', 'challenger_signals_today', 'leaderboard', 'winner', 'n_variants_tracked', 'summary_used_for_challengers', 'duration_s']
- `13:08:28`     {"as_of": "2026-05-04T12:42:29.278233+00:00", "challenger_signals_today": {"challenger_a": {"error": "no_response"}, "challenger_b": {"error": "no_response"}}, "leaderboard": [], "winner": null, "n_variants_tracked": 0, "summary_used_for_challengers": {"khalid_index": 48, "regime": null, "vix": null, "spy_close": null, "macro_surprise_composite": 1.1, "macro_regime": "GROWTH_SURPRISE_POSITIVE", "y
