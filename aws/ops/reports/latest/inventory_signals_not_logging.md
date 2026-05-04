# Step 1 — DDB justhodl-signals: signal_types observed in last 30d

**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-05-04T18:02:24+00:00  

## Log
- `18:02:24`   7 pages, 3858 signals total in last 30d
- `18:02:24`     screener_top_pick                    n=1935
- `18:02:24`     edge_regime                          n=129
- `18:02:24`     market_phase                         n=129
- `18:02:24`     carry_risk                           n=129
- `18:02:24`     edge_composite                       n=129
- `18:02:24`     ml_risk                              n=129
- `18:02:24`     plumbing_stress                      n=129
- `18:02:24`     khalid_index                         n=129
- `18:02:24`     crypto_risk_score                    n=129
- `18:02:24`     crypto_fear_greed                    n=128
- `18:02:24`     momentum_uso                         n=85
- `18:02:24`     momentum_gld                         n=39
- `18:02:24`     crisis_sloos_tighten                 n=31
- `18:02:24`     crisis_hy_oas_vs_spy                 n=31
- `18:02:24`     corr_break_top_pair                  n=31
- `18:02:24`     crisis_sofr_iorb                     n=31
- `18:02:24`     crisis_index_kcfsi                   n=31
- `18:02:24`     crisis_hy_oas_vs_hyg                 n=31
- `18:02:24`     corr_break_composite_vs_vxx          n=31
- `18:02:24`     corr_break_composite_vs_spy          n=31
- `18:02:24`     crisis_dfii10_vs_gld                 n=30
- `18:02:24`     crisis_broad_dollar_vs_eem           n=30
- `18:02:24`     crisis_rate_diff_eur_3m              n=30
- `18:02:24`     crisis_broad_dollar_vs_spy           n=30
- `18:02:24`     crisis_dfii10_vs_spy                 n=30
- `18:02:24`     crisis_ig_bbb_oas                    n=30
- `18:02:24`     crisis_obfr_iorb                     n=30
- `18:02:24`     crisis_t10yie_extreme                n=30
- `18:02:24`     crisis_rate_diff_jpy_3m              n=30
- `18:02:24`     crisis_index_nfci                    n=29
- `18:02:24`     crisis_index_stlfsi4                 n=29
- `18:02:24`     crisis_index_anfci                   n=28
- `18:02:24`     momentum_spy                         n=23
- `18:02:24`     momentum_tlt                         n=9
- `18:02:24`     momentum_uup                         n=3
# Step 2 — Wave 1+2 outputs that should be logging signals

- `18:02:24`   13 S3 outputs to consider:
- `18:02:24`     data/earnings-tracker.json                    29,894b  → would log: earnings_pead, earnings_drift  ⛔ NOT logging
- `18:02:24`     data/short-interest.json                      58,474b  → would log: squeeze_risk  ⛔ NOT logging
- `18:02:24`     data/etf-flows.json                           28,038b  → would log: etf_flow_extreme  ⛔ NOT logging
- `18:02:24`     data/macro-surprise.json                      11,259b  → would log: macro_composite_z  ⛔ NOT logging
- `18:02:24`     data/yield-curve.json                          4,699b  → would log: yc_regime  ⛔ NOT logging
- `18:02:24`     data/historical-analogs.json                   5,790b  → would log: analog_signal  ⛔ NOT logging
- `18:02:24`     data/event-study.json                         11,318b  → would log: event_signal  ⛔ NOT logging
- `18:02:24`     data/correlation-surface.json                 38,656b  → would log: corr_break  ✅ logging
- `18:02:24`     data/auction-crisis.json                      11,576b  → would log: auction_crisis_score  ⛔ NOT logging
- `18:02:24`     ✗ data/eurodollar-stress.json An error occurred (404) when calling the HeadObject operation: Not Found
- `18:02:24`     data/sector-rotation.json                     13,441b  → would log: sector_breadth  ⛔ NOT logging
- `18:02:24`     data/momentum-scanner.json                    82,373b  → would log: momentum_top_pick  ⛔ NOT logging
- `18:02:24`     data/calibration-snapshot.json                32,228b  → would log: (meta — N/A)  ⛔ NOT logging
# Step 3 — Mismatch summary

- `18:02:24`   expected new signal_types not yet logging: 12
- `18:02:24`     • analog_signal
- `18:02:24`     • auction_crisis_score
- `18:02:24`     • corr_break
- `18:02:24`     • earnings_pead
- `18:02:24`     • etf_flow_extreme
- `18:02:24`     • eurodollar_stress
- `18:02:24`     • event_signal
- `18:02:24`     • macro_composite_z
- `18:02:24`     • momentum_top_pick
- `18:02:24`     • sector_breadth
- `18:02:24`     • squeeze_risk
- `18:02:24`     • yc_regime
- `18:02:24`   expected and already logging: 0
