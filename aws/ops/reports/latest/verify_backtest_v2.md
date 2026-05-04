# 0) Wait for any pending update

**Status:** success  
**Duration:** 7.9s  
**Finished:** 2026-05-04T23:12:16+00:00  

## Log
- `23:12:09`   ready, mod=2026-05-04T23:11:57.000+0000
# 1) Force redeploy with 2% position sizing fix

- `23:12:09`   zip size: 4,636b
- `23:12:13` ✅   ✓ deployed, mod=2026-05-04T23:12:09.000+0000
# 2) Inspect deployed source for POSITION_SIZE

- `23:12:14`   ✓ POSITION_SIZE = 0.02
- `23:12:14`   ✓ POSITION_SIZE × w × sign × ret
- `23:12:14`   ✓ 2pct_sizing label
# 3) Re-invoke with the fix

- `23:12:16`   status: 200, duration: 2.1s
- `23:12:16`   n_outcomes: 1600
- `23:12:16`   total_return_pct: 556.6981%
- `23:12:16`   final_nav: $656698.15
- `23:12:16`   max_dd_pct: 0.0%
- `23:12:16`   sharpe: 43.9055
- `23:12:16`   n_signals: 21
# 4) Top 5 + bottom 5 contributors with realistic math

- `23:12:16`   Window: 2026-04-26 → 2026-05-04 (9 days)
- `23:12:16`   Win rate: 54.3% (869/1600)
- `23:12:16`   Final NAV: $656698.15  (return: +556.6981%)
- `23:12:16`   Max DD: 0.00%
- `23:12:16`   Sharpe proxy: 43.9055
- `23:12:16` 
- `23:12:16`   Top 8 contributors:
- `23:12:16`     screener_top_pick                 w=1.334  n= 555  win= 83.2%  contrib=+198.802%
- `23:12:16`     ml_risk                           w=1.385  n=  75  win= 80.0%  contrib=+11.370%
- `23:12:16`     carry_risk                        w=1.453  n=  30  win=100.0%  contrib=+9.973%
- `23:12:16`     plumbing_stress                   w=0.937  n= 119  win= 63.9%  contrib=+3.397%
- `23:12:16`     crypto_fear_greed                 w=0.829  n= 149  win= 58.4%  contrib=+2.999%
- `23:12:16`     momentum_spy                      w=1.254  n=  15  win= 66.7%  contrib=+0.241%
- `23:12:16`     momentum_uup                      w=0.620  n=   4  win=  0.0%  contrib=-0.015%
- `23:12:16`     momentum_tlt                      w=0.500  n=   5  win=  0.0%  contrib=-0.035%
- `23:12:16` 
- `23:12:16`   Bottom 5:
- `23:12:16`     edge_composite                    w=0.474  n=  89  win= 40.5%  contrib=-0.867%
- `23:12:16`     market_phase                      w=0.310  n=  30  win=  0.0%  contrib=-2.126%
- `23:12:16`     edge_regime                       w=0.310  n=  30  win=  0.0%  contrib=-2.126%
- `23:12:16`     crypto_risk_score                 w=0.360  n= 150  win= 18.7%  contrib=-2.186%
- `23:12:16`     khalid_index                      w=0.310  n=  75  win=  1.3%  contrib=-2.683%
- `23:12:16` 
- `23:12:16`   NAV curve sample:
- `23:12:16`     2026-04-26: NAV=$111109.4  daily=+11.109%  cum=+11.109%
- `23:12:16`     2026-04-27: NAV=$158117.9  daily=+42.308%  cum=+53.418%
- `23:12:16`     2026-04-28: NAV=$186686.17  daily=+18.068%  cum=+71.485%
- `23:12:16`     ...
- `23:12:16`     2026-05-01: NAV=$384759.17  daily=+26.140%  cum=+153.715%
- `23:12:16`     2026-05-03: NAV=$484491.83  daily=+25.921%  cum=+179.636%
- `23:12:16`     2026-05-04: NAV=$656698.15  daily=+35.544%  cum=+215.179%
# 5) backtest.html live check

- `23:12:16`   ✓ 200, 22,370b
- `23:12:16`     ✓ title
- `23:12:16`     ✓ nav active
- `23:12:16`     ✓ KPI row
- `23:12:16`     ✓ NAV chart
- `23:12:16`     ✓ contributors
- `23:12:16`     ✓ signal table
- `23:12:16`     ✓ 2% sizing in method
# 6) Backtest tab visible on key pages

- `23:12:16`   ✓ today.html                 Backtest link: True
- `23:12:16`   ✓ brief.html                 Backtest link: True
- `23:12:16`   ✓ calls.html                 Backtest link: True
- `23:12:16`   ✓ performance.html           Backtest link: True
- `23:12:16`   ✓ weights.html               Backtest link: True
