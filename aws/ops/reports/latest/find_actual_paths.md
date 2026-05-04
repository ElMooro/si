# divergence/current.json full content

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-05-04T19:13:57+00:00  

## Log
- `19:13:57` {
  "as_of": "2026-05-04T13:00:37.001001+00:00",
  "v": "1.0",
  "summary": {
    "n_relationships_total": 12,
    "n_processed": 12,
    "n_missing_data": 0,
    "n_extreme": 1,
    "n_alert_worthy": 0
  },
  "relationships": [
    {
      "id": "nasdaq_long_rates",
      "name": "Nasdaq vs 10Y Yield",
      "description": "Rising 10Y compresses growth multiples",
      "status": "ok",
      "z_score": 2.16,
      "extreme": true,
      "alert_worthy": false,
      "mispricing": "QQQ appears RICH vs DGS10",
      "asset_a": "stocks:QQQ",
      "asset_b": "fred:DGS10",
      "a_value": 674.15,
      "b_value": 4.4,
      "today_residual": 68.9883,
      "slope": -194.0834,
      "intercept": 1459.1287,
      "r_squared": 0.066,
      "expected_sign": -1,
      "actual_sign": -1,
      "relationship_intact": true,
      "window_days": 60
    },
    {
      "id": "smallcap_curve",
      "name": "Small Caps vs 2s10s Curve",
      "description": "Small caps benefit from steepening curve (bank NIMs)",
      "status": "ok",
      "z_score": 1.544,
      "extreme": false,
      "alert_worthy": false,
      "mispricing": "IWM appears RICH vs T10Y2Y",
      "asset_a": "stocks:IWM",
      "asset_b": "fred:T10Y2Y",
      "a_value": 279.28,
      "b_value": 0.51,
      "today_residual": 18.2293,
      "slope": 182.740385,
      "intercept": 167.8531,
      "r_squared": 0.093,
      "expected_sign": 1,
      "actual_sign": 1,
      "relationship_intact": true,
      "window_days": 60
    },
    {
      "id": "tips_nominal",
      "name": "TIP vs IEF",
      "description": "Divergence flags inflation regime change",
      "status": "ok",
      "z_score": 1.468,
      "extreme": false,
      "alert_worthy": false,
      "mispricing": "TIP appears RICH vs IEF",
      "asset_a": "stocks:TIP",
      "asset_b": "stocks:IEF",
      "a_value": 111.35,
      "b_value": 94.74,
      "today_residual": 0.6359,
      "slope": 0.29897,
      "intercept": 82.3897,
      "r_squared": 0.251,
      "expected_sign": 1,
      "actual_sign": 1,
      "relationship_intact": true,
      "window_days": 60
    },
    {
      "id": "healthcare_market",
      "name": "XLV vs SPY",
      "description": "Healthcare defensive beta check",
      "status": "ok",
      "z_score": -1.294,
      "extreme": false,
      "alert_worthy": false,
      "mispricing": "XLV appears CHEAP vs SPY",
      "asset_a": "stocks:XLV",
      "asset_b": "stocks:SPY",
      "a_value": 145.16,
      "b_value": 720.65,
      "today_residual": -6.5683,
      "slope": 0.029337,
      "intercept": 130.5867,
      "r_squared": 0.014,
      "expected_sign": 1,
      "actual_sign": 1,
      "relationship_intact": true,
      "window_days": 60
    },
    {
      "id": "gold_real_rates",
      "name": "Gold vs Real Rates",
      "description": "Gold should fall when real rates rise (opportunity cost)",
      "status": "ok",
      "z_score": -1.042,
      "extreme": false,
      "alert_worthy": false,
      "mispricing": "
# S3 keys matching 'cot' or 'extreme'

- `19:13:57`   cot/extremes/current.json                               8,335b  mod=2026-05-01T19:00:49+00:00
- `19:13:57`   cot/history/6B.json                                    19,426b  mod=2026-05-01T19:00:41+00:00
- `19:13:57`   cot/history/6C.json                                    19,428b  mod=2026-05-01T19:00:42+00:00
- `19:13:57`   cot/history/6E.json                                    19,420b  mod=2026-05-01T19:00:41+00:00
- `19:13:57`   cot/history/6J.json                                    19,425b  mod=2026-05-01T19:00:41+00:00
- `19:13:57`   cot/history/6S.json                                    19,168b  mod=2026-05-01T19:00:42+00:00
- `19:13:57`   cot/history/CL.json                                    21,969b  mod=2026-05-01T19:00:43+00:00
- `19:13:57`   cot/history/CT.json                                    21,715b  mod=2026-05-01T19:00:47+00:00
- `19:13:57`   cot/history/DX.json                                    19,164b  mod=2026-05-01T19:00:42+00:00
- `19:13:57`   cot/history/ES.json                                    19,695b  mod=2026-05-01T19:00:38+00:00
- `19:13:57`   cot/history/GC.json                                    21,661b  mod=2026-05-01T19:00:44+00:00
- `19:13:57`   cot/history/HG.json                                    21,563b  mod=2026-05-01T19:00:45+00:00
- `19:13:57`   cot/history/HO.json                                    21,550b  mod=2026-05-01T19:00:44+00:00
- `19:13:57`   cot/history/KC.json                                    21,568b  mod=2026-05-01T19:00:49+00:00
- `19:13:57`   cot/history/NG.json                                    22,128b  mod=2026-05-01T19:00:43+00:00
- `19:13:57`   cot/history/NQ.json                                    19,434b  mod=2026-05-01T19:00:38+00:00
- `19:13:57`   cot/history/PL.json                                    21,275b  mod=2026-05-01T19:00:45+00:00
- `19:13:57`   cot/history/RB.json                                    21,505b  mod=2026-05-01T19:00:43+00:00
- `19:13:57`   cot/history/RTY.json                                   19,437b  mod=2026-05-01T19:00:39+00:00
- `19:13:57`   cot/history/SB.json                                    21,808b  mod=2026-05-01T19:00:49+00:00
# S3 keys matching 'eurodollar'

# justhodl-cot-extremes-scanner code excerpt

- `19:13:57`   state=Active mod=2026-04-25T16:11:09.164+0000
# justhodl-eurodollar-stress Lambda

- `19:13:57`   ✗ An error occurred (ResourceNotFoundException) when calling the GetFunctionConfiguration operation: Function not found: arn:aws:lambda:us-east-1:857687956942:function:justhodl-eurodollar-stress
