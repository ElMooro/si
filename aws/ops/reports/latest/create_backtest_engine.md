# 1) Create / update justhodl-backtest-engine

**Status:** success  
**Duration:** 5.5s  
**Finished:** 2026-05-04T23:07:21+00:00  

## Log
- `23:07:15`   zip size: 4,320b
- `23:07:16` ✅   ✓ created
- `23:07:18`   state: Active mod=2026-05-04T23:07:15.929+0000
# 2) EventBridge — rate(6 hours)

- `23:07:18` ✅   ✓ justhodl-backtest-engine-6h → rate(6 hours)
# 3) Smoke invoke — first backtest run

- `23:07:20`   status: 200, duration: 2.2s
- `23:07:20`   n_outcomes: 1600
- `23:07:20`   total_return_pct: 113653490628.696%
- `23:07:20`   final_nav: $113653490728696.0
- `23:07:20`   max_dd_pct: 0.0%
- `23:07:20`   sharpe: 43.9054
- `23:07:20`   n_signals: 21
- `23:07:20`   duration_s: 1.08
# 4) Verify outputs

- `23:07:21`   ✓ backtest/results.json: 5,862b mod=2026-05-04T23:07:21+00:00
- `23:07:21`   ✓ backtest/summary.json: 2,289b mod=2026-05-04T23:07:21+00:00
# 5) Top 5 contributors and bottom 5

- `23:07:21`   Window: 2026-04-26 → 2026-05-04 (9 days)
- `23:07:21`   Win rate: 54.3% (869/1600)
- `23:07:21`   Final NAV: $113653490728696.0  (+113653490628.70%)
- `23:07:21`   Max DD: 0.00%
- `23:07:21`   Sharpe proxy: 43.9054
- `23:07:21` 
- `23:07:21`   Top 5 contributors:
- `23:07:21`     screener_top_pick                 w=1.334  n= 555  win= 83.2%  contrib=+9940.10%
- `23:07:21`     ml_risk                           w=1.385  n=  75  win= 80.0%  contrib=+568.52%
- `23:07:21`     carry_risk                        w=1.453  n=  30  win=100.0%  contrib=+498.64%
- `23:07:21`     plumbing_stress                   w=0.937  n= 119  win= 63.9%  contrib=+169.83%
- `23:07:21`     crypto_fear_greed                 w=0.829  n= 149  win= 58.4%  contrib=+149.97%
- `23:07:21` 
- `23:07:21`   Bottom 5:
- `23:07:21`     edge_composite                    w=0.474  n=  89  win= 40.5%  contrib=-43.36%
- `23:07:21`     market_phase                      w=0.310  n=  30  win=  0.0%  contrib=-106.32%
- `23:07:21`     edge_regime                       w=0.310  n=  30  win=  0.0%  contrib=-106.32%
- `23:07:21`     crypto_risk_score                 w=0.360  n= 150  win= 18.7%  contrib=-109.29%
- `23:07:21`     khalid_index                      w=0.310  n=  75  win=  1.3%  contrib=-134.12%
# 6) Wire Backtest tab into nav

- `23:07:21`   patched: 20/22
- `23:07:21`     13f.html                   ok_modern
- `23:07:21`     accuracy.html              ok_modern
- `23:07:21`     allocator.html             ok_modern
- `23:07:21`     backtest.html              already_has
- `23:07:21`     brief.html                 ok_modern
- `23:07:21`     calls.html                 ok_modern
- `23:07:21`     desk.html                  ok_topnav
- `23:07:21`     edge.html                  ok_topnav
- `23:07:21`     feedback.html              ok_modern
- `23:07:21`     insiders.html              ok_topnav
- `23:07:21`     intelligence.html          no_match
- `23:07:21`     momentum.html              ok_modern
- `23:07:21`     news.html                  ok_modern
- `23:07:21`     performance.html           ok_modern
- `23:07:21`     read.html                  ok_topnav
- `23:07:21`     research.html              ok_modern
- `23:07:21`     sectors.html               ok_modern
- `23:07:21`     signals.html               ok_topnav
- `23:07:21`     ticker.html                ok_modern
- `23:07:21`     today.html                 ok_modern
- `23:07:21`     vol.html                   ok_modern
- `23:07:21`     weights.html               ok_modern
