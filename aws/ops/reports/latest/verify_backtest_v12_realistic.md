# 1) Force redeploy backtest-engine v1.2

**Status:** success  
**Duration:** 8.5s  
**Finished:** 2026-05-05T13:04:40+00:00  

## Log
- `13:04:32`   zip size: 9,294b
- `13:04:37` ✅   ✓ deployed, mod=2026-05-05T13:04:32.000+0000
# 2) Verify v1.2 code in deployed Lambda

- `13:04:37`   ✓ SLIPPAGE_BPS_PER_LEG = 5
- `13:04:37`   ✓ CONCENTRATION_CAP = 0.40
- `13:04:37`   ✓ GROSS_EXPOSURE_CAP = 1.00
- `13:04:37`   ✓ realistic_results loop
- `13:04:37`   ✓ v1.2 method string
- `13:04:37`   ✓ realistic_summary in output
- `13:04:37`   ✓ realistic_nav_curve in output
- `13:04:37`   ✓ v=1.2 marker
# 3) Invoke backtest engine — full v1.1 + v1.2 pass

- `13:04:40`   status: 200, duration: 2.5s
- `13:04:40` 
- `13:04:40`   ── v1.1 IDEALIZED ──
- `13:04:40`     n_outcomes:           1417
- `13:04:40`     total_return_pct:     75.5027
- `13:04:40`     final_nav:            175502.68
- `13:04:40`     sharpe:               10.1704
- `13:04:40`     max_dd_pct:           0.5873
- `13:04:40`     alpha_vs_spy_pct:     66.6803
- `13:04:40`     n_horizon_weighted:   1415
- `13:04:40`     n_flat_weighted:      2
- `13:04:40` 
- `13:04:40`   ── v1.2 REALISTIC ──
- `13:04:40`     realistic_return_pct:        73.9499
- `13:04:40`     realistic_sharpe:            10.1195
- `13:04:40`     realistic_max_dd_pct:        0.7082
- `13:04:40`     realistic_alpha_pct:         65.1275
- `13:04:40`     friction_drag_pct:           1.5527
- `13:04:40`     n_concentration_capped_days: 9
- `13:04:40`     n_gross_capped_days:         0
# 4) Verify S3 backtest/results.json has realistic_summary

- `13:04:40`   v: 1.2
- `13:04:40`   method: calibrated_alpha_replay_v3_horizon_aware_realistic
- `13:04:40`   constants: {'POSITION_SIZE': 0.005, 'SLIPPAGE_BPS_PER_LEG': 5, 'CONCENTRATION_CAP': 0.4, 'GROSS_EXPOSURE_CAP': 1.0}
- `13:04:40` 
- `13:04:40`   Side-by-side comparison:
- `13:04:40`     Metric                       IDEALIZED v1.1     REALISTIC v1.2     Δ         
- `13:04:40`     ────────────────────────────────────────────────────────────────────────
- `13:04:40`     Total Return %               75.5027            73.9499            -1.5528   
- `13:04:40`     Final NAV $                  175502.68          173949.93          -1552.7500
- `13:04:40`     Sharpe Proxy                 10.1704            10.1195            -0.0509   
- `13:04:40`     Max Drawdown %               0.5873             0.7082             +0.1209   
- `13:04:40`     Alpha vs SPY %               66.6803            65.1275            -1.5528   
- `13:04:40` 
- `13:04:40`   total_slippage_cost_pct:     0.7011
- `13:04:40`   n_concentration_capped_days: 9 of 35 total
- `13:04:40`   n_gross_capped_days:         0 of 35 total
- `13:04:40`   friction_drag_pct:           1.5527
- `13:04:40`   n_trades realistic:          1417
- `13:04:40` 
- `13:04:40`   realistic_nav_curve length: 35
- `13:04:40`   nav_curve length:           35
- `13:04:40` 
- `13:04:40`   Last 3 nav points:
- `13:04:40`     IDEAL  2026-05-02 nav=$175617.34 cum_pct=57.8899 spy_nav=$109222.49
- `13:04:40`     IDEAL  2026-05-03 nav=$175539.01 cum_pct=57.8453 spy_nav=$109222.49
- `13:04:40`     IDEAL  2026-05-04 nav=$175502.68 cum_pct=57.8246 spy_nav=$108822.37
- `13:04:40`     REAL   2026-05-02 nav=$174079.43 cum_pct=56.9718 spy_nav=$109222.49
- `13:04:40`     REAL   2026-05-03 nav=$173991.34 cum_pct=56.9212 spy_nav=$109222.49
- `13:04:40`     REAL   2026-05-04 nav=$173949.93 cum_pct=56.8974 spy_nav=$108822.37
# 5) Verify backtest/summary.json (slim) has realistic_summary

- `13:04:40`   v:                     1.2
- `13:04:40`   has realistic_summary: True
- `13:04:40`   has constants:         True
- `13:04:40`   size:                  3,473 chars
# 6) Realistic vs Idealized — interpretation

- `13:04:40`   v1.2 should show a meaningful drop in Sharpe and total return
- `13:04:40`   vs v1.1, primarily because gross-exposure-cap and concentration-
- `13:04:40`   cap force the system to scale back when 200+ signals fire on the
- `13:04:40`   same day. Slippage is a smaller drag (~10bps × deployed gross).
