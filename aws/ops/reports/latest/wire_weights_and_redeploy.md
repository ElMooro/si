# 1) Redeploy snapshotter with accuracy-dict fix

**Status:** success  
**Duration:** 3.9s  
**Finished:** 2026-05-04T20:51:10+00:00  

## Log
- `20:51:06`   zip size: 3,187b
- `20:51:08` ✅   ✓ deployed 2026-05-04T20:51:06.000+0000
# 2) Reseed first snapshot

- `20:51:09`   status: 200, duration: 1.2s
- `20:51:09`   resp: {"statusCode": 200, "body": "{\"iso_week\": \"2026-W19\", \"n_weights\": 32, \"n_calibrated_n30\": 12, \"n_snapshots_total\": 1, \"duration_s\": 0.25}"}
# 3) Verify snapshot outputs

- `20:51:10`   ✓ calibration/history-index.json: 1 snapshot(s)
- `20:51:10`     • 2026-W19 (2026-05-04 → 2026-05-10)  n_weights=32  n≥30=12
- `20:51:10`   ✓ calibration/latest.json
- `20:51:10`     iso_week: 2026-W19
- `20:51:10`     n_weights: 32
- `20:51:10`     n_calibrated_n30: 12
- `20:51:10`     highest_weight: {'signal': 'crisis_hy_oas_vs_hyg', 'weight': 1.4159}
- `20:51:10`     median_weight: 0.65
- `20:51:10`     weighted_mean_accuracy: 0.3508
- `20:51:10` 
- `20:51:10`     Top 8 weights with accuracy:
- `20:51:10`       crisis_hy_oas_vs_hyg              w=1.416  acc= 92.3%  n_60d=13
- `20:51:10`       screener_top_pick                 w=0.850  acc=     —  n_60d=450
- `20:51:10`       edge_composite                    w=0.832  acc= 57.1%  n_60d=74
- `20:51:10`       valuation_composite               w=0.800  acc=     —  n_60d=0
- `20:51:10`       cftc_gold                         w=0.800  acc=     —  n_60d=0
- `20:51:10`       cftc_spx                          w=0.800  acc=     —  n_60d=0
- `20:51:10`       cftc_bitcoin                      w=0.750  acc=     —  n_60d=0
- `20:51:10`       edge_regime                       w=0.750  acc=     —  n_60d=30
# 4) Wire Weights tab into nav

- `20:51:10`   patched: 19
- `20:51:10`     13f.html                   ok_modern
- `20:51:10`     accuracy.html              ok_modern
- `20:51:10`     allocator.html             ok_modern
- `20:51:10`     brief.html                 ok_modern
- `20:51:10`     desk.html                  ok_topnav
- `20:51:10`     edge.html                  ok_topnav
- `20:51:10`     feedback.html              ok_modern
- `20:51:10`     insiders.html              ok_topnav
- `20:51:10`     intelligence.html          ok_emoji
- `20:51:10`     momentum.html              ok_modern
- `20:51:10`     news.html                  ok_modern
- `20:51:10`     performance.html           ok_modern
- `20:51:10`     read.html                  ok_topnav
- `20:51:10`     research.html              ok_modern
- `20:51:10`     sectors.html               ok_modern
- `20:51:10`     signals.html               ok_topnav
- `20:51:10`     ticker.html                ok_modern
- `20:51:10`     today.html                 ok_modern
- `20:51:10`     vol.html                   ok_modern
- `20:51:10`     weights.html               already_has
