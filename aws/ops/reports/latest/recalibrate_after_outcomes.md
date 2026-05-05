# 1) Calibration state BEFORE

**Status:** success  
**Duration:** 7.2s  
**Finished:** 2026-05-05T12:03:58+00:00  

## Log
- `12:03:51`   generated_at:        2026-05-05T10:40:14.356705+00:00
- `12:03:51`   total_outcomes:      1620
- `12:03:51`   signal_types_tracked: 26
# 2) Invoke calibrator

- `12:03:55`   status: 200, duration: 4.1s
- `12:03:55`   total_outcomes:    1716
- `12:03:55`   n_horizon_lift:    8
# 3) Calibration state AFTER + uplift diff

- `12:03:56`   generated_at:        2026-05-05T12:03:53.040199+00:00
- `12:03:56`   total_outcomes:      1716  (+96)
- `12:03:56`   signal_types_tracked: 26
- `12:03:56` 
- `12:03:56`   Flat weight changes (≥0.05 delta):
- `12:03:56`     ↓ crisis_broad_dollar_vs_eem      0.540 → 0.460  (Δ-0.080)
- `12:03:56`     ↓ crisis_broad_dollar_vs_spy      0.540 → 0.460  (Δ-0.080)
- `12:03:56`     ↑ crisis_hy_oas_vs_spy            0.620 → 0.880  (Δ+0.260)
- `12:03:56`     ↑ crisis_obfr_iorb                0.403 → 0.512  (Δ+0.109)
- `12:03:56`     ↓ crisis_rate_diff_eur_3m         0.540 → 0.460  (Δ-0.080)
- `12:03:56`     ↓ crisis_rate_diff_jpy_3m         0.540 → 0.460  (Δ-0.080)
- `12:03:56`     ↑ crisis_sofr_iorb                0.394 → 0.492  (Δ+0.098)
- `12:03:56` 
- `12:03:56`   Newly-measured (signal, horizon) pairs (n>=5):
- `12:03:56`     crisis_broad_dollar_vs_eem      day_7: w=0.460, acc=0%, n=6  ★ just crossed n=5
- `12:03:56`     crisis_broad_dollar_vs_spy      day_7: w=0.460, acc=0%, n=6  ★ just crossed n=5
- `12:03:56`     crisis_hy_oas_vs_hyg            day_7: w=0.460, acc=0%, n=6  ★ just crossed n=5
- `12:03:56`     crisis_hy_oas_vs_spy            day_7: w=0.880, acc=50%, n=6  ★ just crossed n=5
- `12:03:56`     crisis_obfr_iorb                day_7: w=0.880, acc=50%, n=6  ★ just crossed n=5
- `12:03:56`     crisis_rate_diff_eur_3m         day_7: w=0.460, acc=0%, n=6  ★ just crossed n=5
- `12:03:56`     crisis_rate_diff_jpy_3m         day_7: w=0.460, acc=0%, n=6  ★ just crossed n=5
- `12:03:56`     crisis_sofr_iorb                day_7: w=0.880, acc=50%, n=6  ★ just crossed n=5
- `12:03:56`     momentum_spy                    day_3: w=0.675, acc=40%, n=5  ★ just crossed n=5
- `12:03:56`     momentum_tlt                    day_7: w=0.500, acc=0%, n=5  ★ just crossed n=5
# 4) Re-invoke backtest-engine with updated weights

- `12:03:58`   status: 200, duration: 2.7s
- `12:03:58`   n_outcomes:        1417
- `12:03:58`   total_return_pct:  75.5027%
- `12:03:58`   alpha_vs_spy_pct:  66.6803%
- `12:03:58`   n_horizon_weighted: 1415
- `12:03:58`   horizon_breakdown: {'day_14': 178, 'day_30': 765, 'day_1': 71, 'day_7': 241, 'day_3': 146, 'day_5': 14}
