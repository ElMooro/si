# 1) Invoke justhodl-calibrator (will rescan all outcomes)

**Status:** success  
**Duration:** 4.2s  
**Finished:** 2026-05-04T21:23:42+00:00  

## Log
- `21:23:40`   status: 200, duration: 2.4s
- `21:23:40`   resp head: {"errorMessage": "An error occurred (ValidationException) when calling the PutParameter operation: Standard tier parameters support a maximum parameter value of 4096 characters. To create a larger parameter value, upgrade the parameter to use the advanced-parameter tier. For more information, see https://docs.aws.amazon.com/systems-manager/latest/userguide/parameter-store-advanced-parameters.html", "errorType": "ValidationException", "requestId": "3c4c1f74-9387-44a0-8031-bad142a37695", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 395, in lambda_handler\n    report = run_calibr
- `21:23:40` 
- `21:23:40`   total_outcomes analyzed: None
- `21:23:40`   signals with weights:    0
- `21:23:40` 
- `21:23:40`   screener_top_pick NEW WEIGHT: None  (was 0.85 default)
- `21:23:40` 
- `21:23:40`   Top 8 weights:
# 2) Verify SSM accuracy now has screener_top_pick

- `21:23:40`   accuracy keys: 17
- `21:23:40`   last_modified: 2026-05-04 21:23:40.550000+00:00
- `21:23:40`   ✓ screener_top_pick: {'accuracy': 0.8289, 'n': 450, 'avg_return': 13.5589}
- `21:23:40` 
- `21:23:40`   All accuracy entries:
- `21:23:40`     crisis_hy_oas_vs_hyg                 acc=  92.3%  n=13  avg_ret=-0.2492
- `21:23:40`     screener_top_pick                    acc=  82.9%  n=450  avg_ret=13.5589
- `21:23:40`     edge_composite                       acc=  57.1%  n=49  avg_ret=0.4586
- `21:23:40`     plumbing_stress                      acc=  42.9%  n=49  avg_ret=0.4586
- `21:23:40`     crypto_fear_greed                    acc=  39.7%  n=78  avg_ret=-0.0207
- `21:23:40`     ml_risk                              acc=  33.3%  n=12  avg_ret=0.9399
- `21:23:40`     crypto_risk_score                    acc=  29.5%  n=78  avg_ret=-0.0207
- `21:23:40`     momentum_uso                         acc=  27.3%  n=55  avg_ret=4.3369
- `21:23:40`     momentum_gld                         acc=  18.8%  n=16  avg_ret=0.2588
- `21:23:40`     corr_break_composite_vs_vxx          acc=  16.7%  n=18  avg_ret=-2.4666
- `21:23:40`     crisis_obfr_iorb                     acc=  16.7%  n=12  avg_ret=0.9679
- `21:23:40`     crisis_sofr_iorb                     acc=  15.4%  n=13  avg_ret=1.0067
- `21:23:40`     corr_break_composite_vs_spy          acc=  11.1%  n=18  avg_ret=0.9763
- `21:23:40`     corr_break_top_pair                  acc=   0.0%  n=5  avg_ret=0.8973
- `21:23:40`     khalid_index                         acc=   0.0%  n=12  avg_ret=0.9399
- `21:23:40`     momentum_uup                         acc=   0.0%  n=2  avg_ret=0.1827
- `21:23:40`     momentum_spy                         acc=   0.0%  n=2  avg_ret=0.2769
# 3) Re-run snapshotter to capture updated calibration in this week's snapshot

- `21:23:42`   status: 200, duration: 1.4s
- `21:23:42`   resp: {"statusCode": 200, "body": "{\"iso_week\": \"2026-W19\", \"n_weights\": 32, \"n_calibrated_n30\": 12, \"n_snapshots_total\": 1, \"duration_s\": 0.28}"}
# 4) Pull updated calibration/latest.json

- `21:23:42`   iso_week: 2026-W19
- `21:23:42`   n_weights: 32
- `21:23:42`   highest_weight: {'signal': 'crisis_hy_oas_vs_hyg', 'weight': 1.4159}
- `21:23:42`   median_weight: 0.65
- `21:23:42`   weighted_mean_accuracy: 0.4184
- `21:23:42` 
- `21:23:42`   screener_top_pick in snapshot:
- `21:23:42`     weight:        1.3343
- `21:23:42`     accuracy_meta: {'accuracy': 0.8289, 'n': 450, 'avg_return': 13.5589}
- `21:23:42`     n_outcomes_60d: 450
# 5) Top 10 weights from the new snapshot

- `21:23:42`     crisis_hy_oas_vs_hyg                 w=1.416  acc= 92.3%  n_60d=13   
- `21:23:42`     screener_top_pick                    w=1.334  acc= 82.9%  n_60d=450   ★ NEW
- `21:23:42`     edge_composite                       w=0.832  acc= 57.1%  n_60d=74   
- `21:23:42`     valuation_composite                  w=0.800  acc=     —  n_60d=0    
- `21:23:42`     cftc_gold                            w=0.800  acc=     —  n_60d=0    
- `21:23:42`     cftc_spx                             w=0.800  acc=     —  n_60d=0    
- `21:23:42`     cftc_bitcoin                         w=0.750  acc=     —  n_60d=0    
- `21:23:42`     edge_regime                          w=0.750  acc=     —  n_60d=30   
- `21:23:42`     market_phase                         w=0.750  acc=     —  n_60d=30   
- `21:23:42`     cape_ratio                           w=0.750  acc=     —  n_60d=0    
- `21:23:42`     buffett_indicator                    w=0.750  acc=     —  n_60d=0    
- `21:23:42`     cftc_crude                           w=0.700  acc=     —  n_60d=0    
