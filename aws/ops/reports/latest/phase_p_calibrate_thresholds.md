
# 1) For each pump-list name, find the breakout date and measure signals 30 days earlier

- `00:09:40`     We snapshot the pre-breakout pattern when the move was about to begin.
- `00:09:40`   
- `00:09:40`     AXTI — breakout too early in history (idx=75)
- `00:09:41`     LWLG — breakout too early in history (idx=57)
- `00:09:42`     AAOI — breakout too early in history (idx=43)
- `00:09:42`     AEHR — breakout too early in history (idx=35)
- `00:09:43`     ICHR (final +138%, breakout 2025-08-12):
- `00:09:43`       PRE-BREAKOUT (30d earlier): 2025-06-30 close=$19.64
- `00:09:43`         range_position=25  vol_compression=0.89  obv_slope=0.085
- `00:09:43`         liq_30v60=1.02  liq_30v120=1.02
- `00:09:43`         ret_5d=14.1%  ret_30d=3.5%  ret_60d=-13.1%  ret_120d=0.0%
- `00:09:43`   
- `00:09:43`     MRVL — no clear 5d>25% breakout (smooth uptrend)
- `00:09:44`     INTC (final +122%, breakout 2025-09-24):
- `00:09:44`       PRE-BREAKOUT (30d earlier): 2025-08-12 close=$21.81
- `00:09:44`         range_position=47  vol_compression=1.35  obv_slope=-0.033
- `00:09:44`         liq_30v60=1.11  liq_30v120=1.11
- `00:09:44`         ret_5d=8.0%  ret_30d=-2.6%  ret_60d=1.2%  ret_120d=0.0%
- `00:09:44`   
- `00:09:45`     LITE (final +116%, breakout 2025-10-29):
- `00:09:45`       PRE-BREAKOUT (30d earlier): 2025-09-17 close=$163.34
- `00:09:45`         range_position=94  vol_compression=1.57  obv_slope=0.410
- `00:09:45`         liq_30v60=1.29  liq_30v120=1.40
- `00:09:45`         ret_5d=-0.9%  ret_30d=51.0%  ret_60d=83.2%  ret_120d=147.1%
- `00:09:45`   
- `00:09:45`     CRDO (final +101%, breakout 2025-09-10):
- `00:09:45`       PRE-BREAKOUT (30d earlier): 2025-07-29 close=$109.38
- `00:09:45`         range_position=100  vol_compression=1.52  obv_slope=0.277
- `00:09:45`         liq_30v60=0.89  liq_30v120=0.89
- `00:09:45`         ret_5d=17.7%  ret_30d=48.8%  ret_60d=140.2%  ret_120d=0.0%
- `00:09:45`   

# 2) Aggregate medians across all snapshots

- `00:09:45`     metric                 median   p25      p75      min      max
- `00:09:45`     ---------------------- -------- -------- -------- -------- --------
- `00:09:45`     range_position           +70.50   +47.24  +100.00   +25.39  +100.00
- `00:09:45`     vol_compression           +1.43    +1.35    +1.57    +0.89    +1.57
- `00:09:45`     obv_slope_norm            +0.18    +0.08    +0.41    -0.03    +0.41
- `00:09:45`     liq_30v60                 +1.06    +1.02    +1.29    +0.89    +1.29
- `00:09:45`     liq_30v120                +1.06    +1.02    +1.40    +0.89    +1.40
- `00:09:45`     ret_5d                   +11.07    +8.02   +17.70    -0.93   +17.70
- `00:09:45`     ret_30d                  +26.16    +3.48   +51.03    -2.63   +51.03
- `00:09:45`     ret_60d                  +42.20    +1.21  +140.18   -13.14  +140.18
- `00:09:45`     ret_120d                  +0.00    +0.00  +147.07    +0.00  +147.07

# 3) DERIVED v2 THRESHOLDS

- `00:09:45`     Based on the actual pre-breakout pattern of these winners:
- `00:09:45`   
- `00:09:45`     range_position threshold: < 92.0
- `00:09:45`       (most pump names started somewhere in the lower 60% of their range)
- `00:09:45`   
- `00:09:45`     vol_compression > 1.22
- `00:09:45`       (volatility was modestly compressed — not extreme)
- `00:09:45`   
- `00:09:45`     obv_slope_norm > 0.127
- `00:09:45`       (OBV was rising — accumulation visible)
- `00:09:45`   
- `00:09:45`     liq_30v120 > 0.90
- `00:09:45`       (volume already expanding before breakout)
- `00:09:45`   
- `00:09:45`     ret_60d range: -13% to 140%
- `00:09:45`       (price action varied widely — shouldn't be a hard filter)
- `00:09:45`   
- `00:09:45`     KEY INSIGHT: many names already had +20-50% in 60d before the explosive move.
- `00:09:45`     v1 was wrong to require abs(ret_60d) < 8. v2 should ALLOW positive returns up to ~60%.

# 4) Save calibration results to S3 for v2 use

- `00:09:46`     ✓ wrote data/pre-pump-calibration.json
- `00:09:46`     ── v2 thresholds ──
- `00:09:46`       range_position_max = 91.7
- `00:09:46`       vol_compression_min = 1.22
- `00:09:46`       obv_slope_norm_min = 0.127
- `00:09:46`       liq_30v120_min = 0.9
- `00:09:46`       ret_60d_max = 70.0
- `00:09:46`       ret_60d_min = -25.0
- `00:09:46`       ret_5d_min = -10.0
- `00:09:46`       ret_5d_max = 20.0