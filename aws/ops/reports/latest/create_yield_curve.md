# Create justhodl-yield-curve + smoke test

**Status:** success  
**Duration:** 11.1s  
**Finished:** 2026-05-04T12:19:15+00:00  

## Log
- `12:19:04`   zip size: 4,855b
- `12:19:05` ✅   ✓ created
## EventBridge schedule (6h)

- `12:19:05` ✅   ✓ wired
## Smoke test

- `12:19:15`   status: 200 duration: 2.0s
- `12:19:15`   resp: {"statusCode": 200, "body": "{\"regime\": \"BEAR_STEEPENER\", \"twos_tens_bps\": 52.0, \"butterfly_bps\": -12.0, \"n_signals\": 1, \"duration_s\": 1.21}"}
## S3 verify

- `12:19:15`   as_of: 2026-04-30
- `12:19:15`   regime: BEAR_STEEPENER
- `12:19:15`   desc: long rates rising faster than short — growth/inflation surprise
- `12:19:15`   level: 4.1082%
- `12:19:15`   slope (2s10s): 52.0bps
- `12:19:15`   curvature (butterfly): -24.0bps
## Key spreads (bps)

- `12:19:15`     2s10s                        +52.0 bps
- `12:19:15`     3M10Y                        +72.0 bps
- `12:19:15`     5s30s                        +96.0 bps
- `12:19:15`     2s5s                         +14.0 bps
- `12:19:15`     10s30s                       +58.0 bps
- `12:19:15`     fed_funds_to_10y             +76.0 bps
## Inversion flags

- `12:19:15`     ✓ 2s10s_inverted: False
- `12:19:15`     ✓ 3M10Y_inverted: False
- `12:19:15`     ✓ any_inversion: False
## Curve points (yield + 5d chg)

- `12:19:15`     1M    ( 0.08y)  3.72% chg5d=3.0bps
- `12:19:15`     3M    ( 0.25y)  3.68% chg5d=-1.0bps
- `12:19:15`     6M    ( 0.50y)  3.71% chg5d=-1.0bps
- `12:19:15`     1Y    ( 1.00y)  3.72% chg5d=2.0bps
- `12:19:15`     2Y    ( 2.00y)  3.88% chg5d=5.0bps
- `12:19:15`     3Y    ( 3.00y)  3.91% chg5d=7.0bps
- `12:19:15`     5Y    ( 5.00y)  4.02% chg5d=6.0bps
- `12:19:15`     7Y    ( 7.00y)  4.20% chg5d=7.0bps
- `12:19:15`     10Y   (10.00y)  4.40% chg5d=6.0bps
- `12:19:15`     20Y   (20.00y)  4.97% chg5d=7.0bps
- `12:19:15`     30Y   (30.00y)  4.98% chg5d=6.0bps
## Real yields (TIPS)

- `12:19:15`     5Y_REAL         +1.35% chg5d=-3.0bps
- `12:19:15`     7Y_REAL         +1.64% chg5d=-1.0bps
- `12:19:15`     20Y_REAL        +2.45% chg5d=5.0bps
- `12:19:15`     10Y_REAL        +1.94% chg5d=2.0bps
- `12:19:15`     30Y_REAL        +2.71% chg5d=6.0bps
## Break-evens / inflation expectations

- `12:19:15`     5Y5Y_FORWARD        +2.27% chg5d=4.0bps
- `12:19:15`     5Y_BREAKEVEN        +2.69% chg5d=8.0bps
- `12:19:15`     10Y_BREAKEVEN       +2.48% chg5d=6.0bps
- `12:19:15`   term premium proxy: -2.0bps
## 🔔 SIGNALS (1)

- `12:19:15`     [MEDIUM] bear_steepener                 Bear steepener — long-end selling on growth/inflation surprise
