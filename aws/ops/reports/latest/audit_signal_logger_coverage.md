# DDB justhodl-signals — last 14 days, by signal_type

**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-05-04T19:06:20+00:00  

## Log
- `19:06:20`   screener_top_pick                   n=975
- `19:06:20`   edge_regime                         n=65
- `19:06:20`   edge_composite                      n=65
- `19:06:20`   ml_risk                             n=65
- `19:06:20`   plumbing_stress                     n=65
- `19:06:20`   khalid_index                        n=65
- `19:06:20`   carry_risk                          n=65
- `19:06:20`   crypto_risk_score                   n=65
- `19:06:20`   market_phase                        n=65
- `19:06:20`   crypto_fear_greed                   n=64
- `19:06:20`   momentum_uso                        n=48
- `19:06:20`   crisis_sloos_tighten                n=31
- `19:06:20`   crisis_hy_oas_vs_spy                n=31
- `19:06:20`   corr_break_top_pair                 n=31
- `19:06:20`   crisis_sofr_iorb                    n=31
- `19:06:20`   crisis_index_kcfsi                  n=31
- `19:06:20`   crisis_hy_oas_vs_hyg                n=31
- `19:06:20`   corr_break_composite_vs_vxx         n=31
- `19:06:20`   corr_break_composite_vs_spy         n=31
- `19:06:20`   crisis_dfii10_vs_gld                n=30
- `19:06:20`   crisis_broad_dollar_vs_eem          n=30
- `19:06:20`   crisis_rate_diff_eur_3m             n=30
- `19:06:20`   crisis_broad_dollar_vs_spy          n=30
- `19:06:20`   crisis_dfii10_vs_spy                n=30
- `19:06:20`   crisis_ig_bbb_oas                   n=30
- `19:06:20`   crisis_obfr_iorb                    n=30
- `19:06:20`   crisis_t10yie_extreme               n=30
- `19:06:20`   crisis_rate_diff_jpy_3m             n=30
- `19:06:20`   crisis_index_nfci                   n=29
- `19:06:20`   crisis_index_stlfsi4                n=29
# Wave 1 signal candidates (for calibration scoring)

- `19:06:20`   ✓ earnings_pead                  ALREADY logged (15× in 14d)
- `19:06:20`   ✓ squeeze_risk                   ALREADY logged (12× in 14d)
- `19:06:20`   ✗ etf_flow_extreme               NOT logged   ← from data/etf-flows.json → by_category[*].signal HEAVY_*
- `19:06:20`   ✗ macro_surprise_z               NOT logged   ← from data/macro-surprise.json → composite > 2σ
- `19:06:20`   ✓ yc_regime                      ALREADY logged (5× in 14d)
- `19:06:20`   ✗ correlation_break              NOT logged   ← from data/correlation-surface.json → regime_breaks[]
- `19:06:20`   ✗ auction_crisis_score           NOT logged   ← from data/auction-crisis.json → composite_score > 60
- `19:06:20`   ✗ eurodollar_stress              NOT logged   ← from data/eurodollar-stress.json → composite > 70
- `19:06:20`   ✓ sector_breadth                 ALREADY logged (6× in 14d)
- `19:06:20`   ✓ momentum_top_pick              ALREADY logged (9× in 14d)
- `19:06:20`   ✗ historical_analog              NOT logged   ← from data/historical-analogs.json (mean fwd return)
- `19:06:20`   ✗ event_study                    NOT logged   ← from data/event-study.json (vix_spike, fomc, etc)
- `19:06:20`   ✗ divergence_extreme             NOT logged   ← from divergence/current.json → residual_z >2.5
- `19:06:20`   ✗ cot_extreme                    NOT logged   ← from data/cot-extremes.json → percentile <5 or >95
