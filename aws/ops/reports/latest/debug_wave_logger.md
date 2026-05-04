# Step 1 — DDB scan: any wave-signal-logger-v1 signals in last 30 min?

**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-05-04T18:10:48+00:00  

## Log
- `18:10:48`   total recent items in 30 min: 5
- `18:10:48`   with source=wave-signal-logger-v1: 5
- `18:10:48`     squeeze_risk                   n=2
- `18:10:48`     sector_breadth                 n=1
- `18:10:48`     analog_signal                  n=1
- `18:10:48`     macro_composite_z              n=1
- `18:10:48`     sample: type=sector_breadth pred=DOWN bp=720.65 against=SPY
- `18:10:48`     sample: type=squeeze_risk pred=UP bp=507.92 against=LIN
- `18:10:48`     sample: type=squeeze_risk pred=UP bp=127.67 against=SHOP
# Step 2 — etf-flows schema (find the str-not-dict bug)

- `18:10:48`   top-level keys: ['version', 'generated_at', 'n_etfs_analyzed', 'by_etf', 'by_category', 'heavy_inflow', 'heavy_outflow', 'unusual_vol', 'rotation_in', 'rotation_out', 'duration_s', 'data_sources', 'signal_definitions']
- `18:10:48`   category 'BROAD_EQUITY_US': type=dict len=7
- `18:10:48`     first item type: dict  preview={'category': 'BROAD_EQUITY_US', 'n_etfs': 8, 'total_aum_b': 3708.04, 'total_today_dollar_vol_b': 76.746, 'avg_dvol_z': 0, 'avg_return_1d_pct': 0.4, 'category_signal': 'NORMAL'}
- `18:10:48`     first item keys: ['category', 'n_etfs', 'total_aum_b', 'total_today_dollar_vol_b', 'avg_dvol_z', 'avg_return_1d_pct', 'category_signal']
# Step 3 — yield-curve schema (spreads_bps shape)

- `18:10:48`   spreads_bps: {'2s10s': 52.0, '3M10Y': 72.0, '5s30s': 96.0, '2s5s': 14.0, '10s30s': 58.0, 'fed_funds_to_10y': None}
- `18:10:48`   butterfly_5y_bps: -12.0
- `18:10:48`   inversion_flags: {'2s10s_inverted': False, '3M10Y_inverted': False, 'any_inversion': False}
- `18:10:48`   regime: BEAR_STEEPENER
# Step 4 — momentum-scanner schema

- `18:10:48`   top-level keys: ['version', 'generated_at', 'duration_s', 'universe_size', 'n_with_data', 'fields', 'top_50_composite', 'bottom_50_composite', 'top_20_mom_12_1', 'top_20_acceleration', 'top_20_at_52w_high', 'top_20_vol_adj', 'sector_breakdown']
# Step 5 — earnings-tracker pead_signals shape

- `18:10:48`   pead_signals count: 10
- `18:10:48`     None: 10
# Step 6 — auction-crisis composite_score

- `18:10:48`   composite_score: 14.7  regime: CALM
# Step 7 — historical-analogs directional_call

- `18:10:48`   directional_call: BULLISH
- `18:10:48`   forward_distribution keys: ['5d', '21d', '63d', '126d']
- `18:10:48`   forward_distribution: {"5d": {"n": 15, "mean_pct": 0.09, "median_pct": 0.45, "hit_rate_pct": 60.0, "min_pct": -2.36, "max_pct": 1.78, "stdev_pct": 1.1}, "21d": {"n": 15, "mean_pct": 2.54, "median_pct": 1.96, "hit_rate_pct": 100.0, "min_pct": 0.18, "max_pct": 4.95, "stdev_pct": 1.66}, "63d": {"n": 15, "mean_pct": 8.02, "m
# Step 8 — event-study expected return

- `18:10:48`   expected_21d_return_from_active_pct: None
- `18:10:48`   active_themes: []
