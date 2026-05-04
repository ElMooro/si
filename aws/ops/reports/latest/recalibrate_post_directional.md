# 1) Trigger justhodl-calibrator

**Status:** success  
**Duration:** 4.8s  
**Finished:** 2026-05-04T22:13:47+00:00  

## Log
- `22:13:45`   status: 200, duration: 3.1s
- `22:13:45`   total_outcomes: 1302
# 2) New SSM accuracy after backfill

- `22:13:45`   accuracy keys: 21 (was 17)
- `22:13:45` 
- `22:13:45`   All entries sorted by accuracy:
- `22:13:45`     carry_risk                           acc= 100.0%  n=30     avg_ret=+11.44%
- `22:13:45`     crisis_hy_oas_vs_hyg                 acc=  92.3%  n=13     avg_ret=-0.25%
- `22:13:45`     ml_risk                              acc=  88.1%  n=67     avg_ret=+6.41%
- `22:13:45`     screener_top_pick                    acc=  82.9%  n=450    avg_ret=+13.56%
- `22:13:45`     momentum_spy                         acc=  76.9%  n=13     avg_ret=+0.75%
- `22:13:45`     plumbing_stress                      acc=  61.5%  n=104    avg_ret=+4.59%
- `22:13:45`     crypto_fear_greed                    acc=  57.0%  n=121    avg_ret=+1.06%
- `22:13:45`     edge_composite                       acc=  37.8%  n=74     avg_ret=+1.43%
- `22:13:45`     momentum_uso                         acc=  36.8%  n=76     avg_ret=+6.26%
- `22:13:45`     momentum_gld                         acc=  24.2%  n=33     avg_ret=-1.09%
- `22:13:45`     crypto_risk_score                    acc=  23.1%  n=121    avg_ret=+1.06%
- `22:13:45`     corr_break_composite_vs_vxx          acc=  16.7%  n=18     avg_ret=-2.47%
- `22:13:45`     crisis_obfr_iorb                     acc=  16.7%  n=12     avg_ret=+0.97%
- `22:13:45`     crisis_sofr_iorb                     acc=  15.4%  n=13     avg_ret=+1.01%
- `22:13:45`     corr_break_composite_vs_spy          acc=  11.1%  n=18     avg_ret=+0.98%
- `22:13:45`     edge_regime                          acc=   0.0%  n=30     avg_ret=+11.44%
- `22:13:45`     khalid_index                         acc=   0.0%  n=67     avg_ret=+6.41%
- `22:13:45`     corr_break_top_pair                  acc=   0.0%  n=5      avg_ret=+0.90%
- `22:13:45`     market_phase                         acc=   0.0%  n=30     avg_ret=+11.44%
- `22:13:45`     momentum_uup                         acc=   0.0%  n=2      avg_ret=+0.18%
- `22:13:45`     momentum_tlt                         acc=   0.0%  n=5      avg_ret=-0.71%
# 3) New SSM weights

- `22:13:45`   weights count: 33
- `22:13:45` 
- `22:13:45`   Top 15 weights:
- `22:13:45`     carry_risk                           w=1.453
- `22:13:45`     crisis_hy_oas_vs_hyg                 w=1.416
- `22:13:45`     ml_risk                              w=1.385
- `22:13:45`     screener_top_pick                    w=1.334
- `22:13:45`     momentum_spy                         w=1.254
- `22:13:45`     plumbing_stress                      w=0.937
- `22:13:45`     crypto_fear_greed                    w=0.829
- `22:13:45`     valuation_composite                  w=0.800
- `22:13:45`     cftc_gold                            w=0.800
- `22:13:45`     cftc_spx                             w=0.800
- `22:13:45`     cftc_bitcoin                         w=0.750
- `22:13:45`     cape_ratio                           w=0.750
- `22:13:45`     buffett_indicator                    w=0.750
- `22:13:45`     cftc_crude                           w=0.700
- `22:13:45`     crypto_btc_signal                    w=0.700
- `22:13:45` 
- `22:13:45`   Bottom 5 weights:
- `22:13:45`     crisis_sofr_iorb                     w=0.333
- `22:13:45`     corr_break_composite_vs_spy          w=0.324
- `22:13:45`     edge_regime                          w=0.310
- `22:13:45`     khalid_index                         w=0.310
- `22:13:45`     market_phase                         w=0.310
# 4) Re-snapshot W19 with the new state

- `22:13:47`   resp: {"statusCode": 200, "body": "{\"iso_week\": \"2026-W19\", \"n_weights\": 33, \"n_calibrated_n30\": 12, \"n_snapshots_total\": 1, \"duration_s\": 0.25}"}
# 5) Final view of 2026-W19 snapshot

- `22:13:47`   highest: {'signal': 'carry_risk', 'weight': 1.453}
- `22:13:47`   median: 0.65
- `22:13:47`   weighted_mean_acc: 0.5527
- `22:13:47`   n_calibrated_n30: 12
- `22:13:47` 
- `22:13:47`   Full ranking by weight:
- `22:13:47`    ★ carry_risk                           w=1.453  acc= 100.0%  n=30     avg_ret=+11.44%
- `22:13:47`      crisis_hy_oas_vs_hyg                 w=1.416  acc=  92.3%  n=13     avg_ret=-0.25%
- `22:13:47`    ★ ml_risk                              w=1.385  acc=  88.1%  n=67     avg_ret=+6.41%
- `22:13:47`    ★ screener_top_pick                    w=1.334  acc=  82.9%  n=450    avg_ret=+13.56%
- `22:13:47`      momentum_spy                         w=1.254  acc=  76.9%  n=13     avg_ret=+0.75%
- `22:13:47`      plumbing_stress                      w=0.937  acc=  61.5%  n=104    avg_ret=+4.59%
- `22:13:47`      crypto_fear_greed                    w=0.829  acc=  57.0%  n=121    avg_ret=+1.06%
- `22:13:47`      valuation_composite                  w=0.800  acc=      —  n=0      avg_ret=—
- `22:13:47`      cftc_gold                            w=0.800  acc=      —  n=0      avg_ret=—
- `22:13:47`      cftc_spx                             w=0.800  acc=      —  n=0      avg_ret=—
- `22:13:47`      cftc_bitcoin                         w=0.750  acc=      —  n=0      avg_ret=—
- `22:13:47`      cape_ratio                           w=0.750  acc=      —  n=0      avg_ret=—
- `22:13:47`      buffett_indicator                    w=0.750  acc=      —  n=0      avg_ret=—
- `22:13:47`      cftc_crude                           w=0.700  acc=      —  n=0      avg_ret=—
- `22:13:47`      crypto_btc_signal                    w=0.700  acc=      —  n=0      avg_ret=—
- `22:13:47`      btc_mvrv                             w=0.700  acc=      —  n=0      avg_ret=—
- `22:13:47`      crypto_eth_signal                    w=0.650  acc=      —  n=0      avg_ret=—
- `22:13:47`      screener_buy                         w=0.650  acc=      —  n=0      avg_ret=—
