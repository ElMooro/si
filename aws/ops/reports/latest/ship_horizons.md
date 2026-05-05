# 1) Wire Horizons tab into nav

**Status:** success  
**Duration:** 10.5s  
**Finished:** 2026-05-05T10:29:51+00:00  

## Log
- `10:29:41`   patched: 21
- `10:29:41`     13f.html                   ok_modern
- `10:29:41`     accuracy.html              ok_modern
- `10:29:41`     allocator.html             ok_modern
- `10:29:41`     backtest.html              ok_modern
- `10:29:41`     brief.html                 ok_modern
- `10:29:41`     calls.html                 ok_modern
- `10:29:41`     desk.html                  ok_topnav
- `10:29:41`     edge.html                  ok_topnav
- `10:29:41`     feedback.html              ok_modern
- `10:29:41`     horizons.html              already_has
- `10:29:41`     insiders.html              ok_topnav
- `10:29:41`     intelligence.html          no_match
- `10:29:41`     momentum.html              ok_modern
- `10:29:41`     news.html                  ok_modern
- `10:29:41`     performance.html           ok_modern
- `10:29:41`     read.html                  ok_topnav
- `10:29:41`     research.html              ok_modern
- `10:29:41`     sectors.html               ok_modern
- `10:29:41`     signals.html               ok_topnav
- `10:29:41`     ticker.html                ok_modern
- `10:29:41`     today.html                 ok_modern
- `10:29:41`     vol.html                   ok_modern
- `10:29:41`     weights.html               ok_modern
# 2) Force redeploy calibrator

- `10:29:41`   zip size: 8,265b
- `10:29:43` ✅   ✓ deployed, mod=2026-05-05T10:29:41.000+0000
# 3) Inspect deployed source for multi-horizon code

- `10:29:44`   ✓ window_weights computation
- `10:29:44`   ✓ recommended_horizon
- `10:29:44`   ✓ per-horizon SSM writes
- `10:29:44`   ✓ horizon_lifts in response
# 4) Manually invoke calibrator to populate horizons

- `10:29:47`   status: 200, duration: 3.5s
- `10:29:47`   total_outcomes:    1620
- `10:29:47`   n_horizon_lift:    13
- `10:29:47`     edge_composite                flat=0.51 → day_1=1.29  (Δ+0.79)
- `10:29:47`     crypto_fear_greed             flat=0.86 → day_14=1.44  (Δ+0.58)
- `10:29:47`     plumbing_stress               flat=0.99 → day_14=1.41  (Δ+0.42)
- `10:29:47`     crisis_sofr_iorb              flat=0.39 → day_7=0.70  (Δ+0.31)
- `10:29:47`     crisis_obfr_iorb              flat=0.40 → day_7=0.70  (Δ+0.30)
- `10:29:47`     crisis_hy_oas_vs_hyg          flat=0.96 → day_3=1.22  (Δ+0.26)
- `10:29:47`     momentum_spy                  flat=1.06 → day_7=1.31  (Δ+0.25)
- `10:29:47`     momentum_uup                  flat=0.54 → day_1=0.70  (Δ+0.16)
# 5) Verify calibration/latest.json has window_weights + recommended_horizon

- `10:29:47`   window_weights:        27 signals
- `10:29:47`   recommended_horizon:   27 signals
- `10:29:47` 
- `10:29:47`   Sample window_weights structure:
- `10:29:47`     edge_composite             day_14=0.31, day_1=1.29, day_7=0.31
- `10:29:47`     screener_top_pick          day_30=1.34
- `10:29:47`     crypto_fear_greed          day_1=0.42, day_14=1.44, day_7=1.06, day_3=0.53
- `10:29:47`     plumbing_stress            day_30=1.06, day_1=0.67, day_7=0.78, day_14=1.41
- `10:29:47`     crypto_risk_score          day_1=0.40, day_3=0.38, day_7=0.33, day_14=0.31
# 6) Verify per-horizon SSM params written

- `10:29:48`   ✓ /justhodl/calibration/weights/day_7: 18 signal types
- `10:29:48`   ✓ /justhodl/calibration/weights/day_30: 7 signal types
- `10:29:48`   - /justhodl/calibration/weights/day_60: not written (An error occurred (ParameterNotFound) when calling the GetPa)
- `10:29:48`   - /justhodl/calibration/weights/day_90: not written (An error occurred (ParameterNotFound) when calling the GetPa)
# 7) Verify horizons.html on production

- `10:29:51`   ✗ HTTP Error 404: Not Found
