# 1) Create justhodl-eurodollar-stress Lambda

**Status:** success  
**Duration:** 6.4s  
**Finished:** 2026-05-04T19:48:18+00:00  

## Log
- `19:48:12`   zip size: 4,374b
- `19:48:12` ✅   ✓ FRED key sourced from justhodl-yield-curve.FRED_KEY, len=32
- `19:48:13` ✅   ✓ created
- `19:48:15`   state: Active, last_update: Successful
# 2) EventBridge schedule (rate(1 hour))

- `19:48:15` ✅   ✓ wired
# 3) Smoke test — first run (will hit FRED 8 times, ~10-30s)

- `19:48:18`   status: 200  duration: 2.5s
- `19:48:18`   resp: {"statusCode": 200, "body": "{\"composite_score\": 41.4, \"severity\": \"CALM\", \"regime\": \"CALM\", \"n_signals_used\": 6, \"duration_s\": 1.62}"}
# 4) S3 verify

- `19:48:18`   composite_score: 41.4
- `19:48:18`   severity: CALM  regime: CALM
- `19:48:18`   n_signals_used: 6/8
- `19:48:18`   duration_s: 1.62
- `19:48:18` 
- `19:48:18`   Signal breakdown:
- `19:48:18`     hy_oas          value=      2.77  score= 11.2/100  ██
- `19:48:18`     ig_oas          value=      0.81  score= 22.8/100  ████
- `19:48:18`     vix             value=     16.99  score= 34.9/100  ██████
- `19:48:18`     broad_dollar    value=  118.7294  score= 34.4/100  ██████
- `19:48:18`     t_bill_3m       value=      3.59  score= 57.0/100  ███████████
- `19:48:18`     repo_spread     value=      0.02  score= 88.1/100  █████████████████
- `19:48:18` 
- `19:48:18`   🔴 hot signals (>=70):
- `19:48:18`     repo_spread     score=88.1 (SOFR – Fed Funds Spread)
- `19:48:18` 
- `19:48:18`   🟢 cold signals (<=30):
- `19:48:18`     hy_oas          score=11.2 (HY Credit OAS)
- `19:48:18`     ig_oas          score=22.8 (IG Credit OAS)
- `19:48:18` 
- `19:48:18`   ⚠ failures:
- `19:48:18`     {'signal': 'ofr_fsi', 'reason': 'HTTP Error 400: Bad Request'}
- `19:48:18`     {'signal': 'rate_vol_10y', 'reason': 'insufficient_data'}
