# 0) Wait for Lambda to stabilize

**Status:** success  
**Duration:** 4.5s  
**Finished:** 2026-05-04T21:30:37+00:00  

## Log
- `21:30:33` ✅   ✓ ready, last_modified=2026-05-04T21:27:30.000+0000
# 1) Invoke calibrator clean (with SSM-summary fix)

- `21:30:35`   status: 200, duration: 2.4s
- `21:30:35`   total_outcomes: 882
- `21:30:35`   n_weights: 33
- `21:30:35` 
- `21:30:35`   Top 8 weights AFTER backfill + recalibration:
- `21:30:35`         crisis_hy_oas_vs_hyg                 w=1.416
- `21:30:35`       ★ screener_top_pick                    w=1.334
- `21:30:35`         edge_composite                       w=0.832
- `21:30:35`         valuation_composite                  w=0.800
- `21:30:35`         cftc_gold                            w=0.800
- `21:30:35`         cftc_spx                             w=0.800
- `21:30:35`         cftc_bitcoin                         w=0.750
- `21:30:35`         edge_regime                          w=0.750
# 2) Verify slim summary now in SSM (was 4KB-failing before)

- `21:30:35`   ✓ size: 388 chars (was crashing >4096)
- `21:30:35`     generated_at              = 2026-05-04T21:30:34.190325+00:00
- `21:30:35`     total_outcomes            = 882
- `21:30:35`     signal_types_tracked      = 17
- `21:30:35`     n_weights                 = 32
- `21:30:35`     n_accuracy                = 17
- `21:30:35`     khalid_weights            = {'core_khalid': 0.675, 'cftc': 0.15, 'edge_regime': 0.1, 'valuation': 0.075}
- `21:30:35`     n_recommendations         = 15
- `21:30:35`     _note                     = Full report at s3://justhodl-dashboard-live/calibration/latest.json (SSM Standar
# 3) Re-snapshot — capture final W19 state into weights ledger

- `21:30:37`   resp: {"statusCode": 200, "body": "{\"iso_week\": \"2026-W19\", \"n_weights\": 32, \"n_calibrated_n30\": 12, \"n_snapshots_total\": 1, \"duration_s\": 0.29}"}
# 4) Final state — top 12 weights in 2026-W19 snapshot

- `21:30:37`   iso_week: 2026-W19
- `21:30:37`   highest_weight: {'signal': 'crisis_hy_oas_vs_hyg', 'weight': 1.4159}
- `21:30:37`   median_weight: 0.65
- `21:30:37`   weighted_mean_accuracy: 0.4184
- `21:30:37`   n_calibrated_n30: 12
- `21:30:37` 
- `21:30:37`     crisis_hy_oas_vs_hyg                 w=1.416  acc=  92.3%  n_60d=13     avg_ret=-0.25%
- `21:30:37`     screener_top_pick                    w=1.334  acc=  82.9%  n_60d=450    avg_ret=+13.56%
- `21:30:37`     edge_composite                       w=0.832  acc=  57.1%  n_60d=74     avg_ret=+0.46%
- `21:30:37`     valuation_composite                  w=0.800  acc=      —  n_60d=0      avg_ret=—
- `21:30:37`     cftc_gold                            w=0.800  acc=      —  n_60d=0      avg_ret=—
- `21:30:37`     cftc_spx                             w=0.800  acc=      —  n_60d=0      avg_ret=—
- `21:30:37`     cftc_bitcoin                         w=0.750  acc=      —  n_60d=0      avg_ret=—
- `21:30:37`     edge_regime                          w=0.750  acc=      —  n_60d=30     avg_ret=—
- `21:30:37`     market_phase                         w=0.750  acc=      —  n_60d=30     avg_ret=—
- `21:30:37`     cape_ratio                           w=0.750  acc=      —  n_60d=0      avg_ret=—
- `21:30:37`     buffett_indicator                    w=0.750  acc=      —  n_60d=0      avg_ret=—
- `21:30:37`     cftc_crude                           w=0.700  acc=      —  n_60d=0      avg_ret=—
