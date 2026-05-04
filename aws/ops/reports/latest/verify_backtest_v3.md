# 0) Wait for any pending update

**Status:** success  
**Duration:** 7.2s  
**Finished:** 2026-05-04T23:17:24+00:00  

## Log
- `23:17:17`   ready, mod=2026-05-04T23:16:45.000+0000
# 1) Force redeploy

- `23:17:22` ✅   ✓ deployed, mod=2026-05-04T23:17:18.000+0000
# 2) Re-invoke

- `23:17:24`   status: 200, duration: 2.1s
- `23:17:24`   n_outcomes: 1331
- `23:17:24`   total_return_pct: 69.018%
- `23:17:24`   final_nav: $169018.0
- `23:17:24`   max_dd_pct: 0.5304%
- `23:17:24`   sharpe: 9.8867
- `23:17:24`   n_signals: 21
# 3) Detailed v3 results

- `23:17:24`   Method: calibrated_alpha_replay_v2
- `23:17:24`   Window: 2026-04-26 → 2026-05-04 (9 days)
- `23:17:24`   N unique trades: 1331 (after dedup)
- `23:17:24`   Win rate: 58.0% (772/1331)
- `23:17:24`   Final NAV: $169018.0  (return: +69.0180%)
- `23:17:24`   Max DD: 0.53%
- `23:17:24`   Sharpe proxy: 9.8867
- `23:17:24` 
- `23:17:24`   Top 8 contributors:
- `23:17:24`     screener_top_pick                 w=1.334  n= 555  win= 83.2%  contrib=+49.700%
- `23:17:24`     ml_risk                           w=1.385  n=  75  win= 80.0%  contrib=+2.843%
- `23:17:24`     carry_risk                        w=1.453  n=  30  win=100.0%  contrib=+2.493%
- `23:17:24`     plumbing_stress                   w=0.937  n=  99  win= 68.7%  contrib=+0.849%
- `23:17:24`     crypto_fear_greed                 w=0.829  n=  84  win= 77.4%  contrib=+0.816%
- `23:17:24`     momentum_spy                      w=1.254  n=  10  win= 80.0%  contrib=+0.043%
- `23:17:24`     momentum_uup                      w=0.620  n=   2  win=  0.0%  contrib=-0.003%
- `23:17:24`     momentum_tlt                      w=0.500  n=   5  win=  0.0%  contrib=-0.009%
- `23:17:24` 
- `23:17:24`   Bottom 5:
- `23:17:24`     edge_composite                    w=0.474  n=  69  win= 23.2%  contrib=-0.223%
- `23:17:24`     crypto_risk_score                 w=0.360  n=  85  win=  8.2%  contrib=-0.462%
- `23:17:24`     market_phase                      w=0.310  n=  30  win=  0.0%  contrib=-0.532%
- `23:17:24`     edge_regime                       w=0.310  n=  30  win=  0.0%  contrib=-0.532%
- `23:17:24`     khalid_index                      w=0.310  n=  75  win=  1.3%  contrib=-0.671%
- `23:17:24` 
- `23:17:24`   NAV curve (full):
- `23:17:24`     2026-03-26: $   101442  daily=+1.442%  cum=+1.442%  (n=21)
- `23:17:24`     2026-03-27: $   106971  daily=+5.450%  cum=+6.892%  (n=21)
- `23:17:24`     2026-03-28: $   113514  daily=+6.117%  cum=+13.009%  (n=21)
- `23:17:24`     2026-03-29: $   118762  daily=+4.623%  cum=+17.632%  (n=21)
- `23:17:24`     2026-03-30: $   124615  daily=+4.929%  cum=+22.561%  (n=21)
- `23:17:24`     ... [23 dates]
- `23:17:24`     2026-04-29: $   169260  daily=-0.021%  cum=+54.145%
- `23:17:24`     2026-04-30: $   169227  daily=-0.019%  cum=+54.125%
- `23:17:24`     2026-05-01: $   169230  daily=+0.002%  cum=+54.127%
- `23:17:24`     2026-05-02: $   169133  daily=-0.058%  cum=+54.069%
- `23:17:24`     2026-05-03: $   169018  daily=-0.068%  cum=+54.001%
