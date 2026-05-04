# 1) divergence/current.json — full structure

**Status:** success  
**Duration:** 13.9s  
**Finished:** 2026-05-04T19:17:45+00:00  

## Log
- `19:17:32`   top keys: ['as_of', 'v', 'summary', 'relationships', 'thresholds']
- `19:17:32`   relationships type: list
- `19:17:32`   count: 12
- `19:17:32`   [0] keys: ['id', 'name', 'description', 'status', 'z_score', 'extreme', 'alert_worthy', 'mispricing', 'asset_a', 'asset_b', 'a_value', 'b_value', 'today_residual', 'slope', 'intercept', 'r_squared', 'expected_sign', 'actual_sign', 'relationship_intact', 'window_days']
- `19:17:32`       sample: {'id': 'nasdaq_long_rates', 'name': 'Nasdaq vs 10Y Yield', 'description': 'Rising 10Y compresses growth multiples', 'status': 'ok', 'z_score': '2.16', 'extreme': 'True', 'alert_worthy': 'False', 'mispricing': 'QQQ appears RICH vs DGS10', 'asset_a': 'stocks:QQQ', 'asset_b': 'fred:DGS10', 'a_value': '674.15', 'b_value': '4.4', 'today_residual': '68.9883', 'slope': '-194.0834', 'intercept': '1459.1287', 'r_squared': '0.066', 'expected_sign': '-1', 'actual_sign': '-1', 'relationship_intact': 'True', 'window_days': '60'}
- `19:17:32`   [1] keys: ['id', 'name', 'description', 'status', 'z_score', 'extreme', 'alert_worthy', 'mispricing', 'asset_a', 'asset_b', 'a_value', 'b_value', 'today_residual', 'slope', 'intercept', 'r_squared', 'expected_sign', 'actual_sign', 'relationship_intact', 'window_days']
- `19:17:32`       sample: {'id': 'smallcap_curve', 'name': 'Small Caps vs 2s10s Curve', 'description': 'Small caps benefit from steepening curve (bank NIMs)', 'status': 'ok', 'z_score': '1.544', 'extreme': 'False', 'alert_worthy': 'False', 'mispricing': 'IWM appears RICH vs T10Y2Y', 'asset_a': 'stocks:IWM', 'asset_b': 'fred:T10Y2Y', 'a_value': '279.28', 'b_value': '0.51', 'today_residual': '18.2293', 'slope': '182.740385', 'intercept': '167.8531', 'r_squared': '0.093', 'expected_sign': '1', 'actual_sign': '1', 'relationship_intact': 'True', 'window_days': '60'}
- `19:17:32`   [2] keys: ['id', 'name', 'description', 'status', 'z_score', 'extreme', 'alert_worthy', 'mispricing', 'asset_a', 'asset_b', 'a_value', 'b_value', 'today_residual', 'slope', 'intercept', 'r_squared', 'expected_sign', 'actual_sign', 'relationship_intact', 'window_days']
- `19:17:32`       sample: {'id': 'tips_nominal', 'name': 'TIP vs IEF', 'description': 'Divergence flags inflation regime change', 'status': 'ok', 'z_score': '1.468', 'extreme': 'False', 'alert_worthy': 'False', 'mispricing': 'TIP appears RICH vs IEF', 'asset_a': 'stocks:TIP', 'asset_b': 'stocks:IEF', 'a_value': '111.35', 'b_value': '94.74', 'today_residual': '0.6359', 'slope': '0.29897', 'intercept': '82.3897', 'r_squared': '0.251', 'expected_sign': '1', 'actual_sign': '1', 'relationship_intact': 'True', 'window_days': '60'}
- `19:17:32`   [3] keys: ['id', 'name', 'description', 'status', 'z_score', 'extreme', 'alert_worthy', 'mispricing', 'asset_a', 'asset_b', 'a_value', 'b_value', 'today_residual', 'slope', 'intercept', 'r_squared', 'expected_sign', 'actual_sign', 'relationship_intact', 'window_days']
- `19:17:32`       sample: {'id': 'healthcare_market', 'name': 'XLV vs SPY', 'description': 'Healthcare defensive beta check', 'status': 'ok', 'z_score': '-1.294', 'extreme': 'False', 'alert_worthy': 'False', 'mispricing': 'XLV appears CHEAP vs SPY', 'asset_a': 'stocks:XLV', 'asset_b': 'stocks:SPY', 'a_value': '145.16', 'b_value': '720.65', 'today_residual': '-6.5683', 'slope': '0.029337', 'intercept': '130.5867', 'r_squared': '0.014', 'expected_sign': '1', 'actual_sign': '1', 'relationship_intact': 'True', 'window_days': '60'}
- `19:17:32`   [4] keys: ['id', 'name', 'description', 'status', 'z_score', 'extreme', 'alert_worthy', 'mispricing', 'asset_a', 'asset_b', 'a_value', 'b_value', 'today_residual', 'slope', 'intercept', 'r_squared', 'expected_sign', 'actual_sign', 'relationship_intact', 'window_days']
- `19:17:32`       sample: {'id': 'gold_real_rates', 'name': 'Gold vs Real Rates', 'description': 'Gold should fall when real rates rise (opportunity cost)', 'status': 'ok', 'z_score': '-1.042', 'extreme': 'False', 'alert_worthy': 'False', 'mispricing': 'GLD appears CHEAP vs real_rate_10y', 'asset_a': 'stocks:GLD', 'asset_b': 'synthetic:real_rate_10y', 'a_value': '423.18', 'b_value': '1.92', 'today_residual': '-9.325', 'slope': '-116.395786', 'intercept': '655.9849', 'r_squared': '0.433', 'expected_sign': '-1', 'actual_sign': '-1', 'relationship_intact': 'True', 'window_days': '60'}
- `19:17:32`   summary: {'n_relationships_total': 12, 'n_processed': 12, 'n_missing_data': 0, 'n_extreme': 1, 'n_alert_worthy': 0}
- `19:17:32`   thresholds: {'z_threshold': 2.0, 'extreme_threshold': 3.0, 'rolling_window_days': 60}
# 2) S3 search: any *cot* or *extreme* keys

- `19:17:32`   cot/extremes/current.json                             8,335b  2026-05-01T19:00:49+00:00
- `19:17:32`   cot/history/6B.json                                  19,426b  2026-05-01T19:00:41+00:00
- `19:17:32`   cot/history/6C.json                                  19,428b  2026-05-01T19:00:42+00:00
- `19:17:32`   cot/history/6E.json                                  19,420b  2026-05-01T19:00:41+00:00
- `19:17:32`   cot/history/6J.json                                  19,425b  2026-05-01T19:00:41+00:00
- `19:17:32`   cot/history/6S.json                                  19,168b  2026-05-01T19:00:42+00:00
- `19:17:32`   cot/history/CL.json                                  21,969b  2026-05-01T19:00:43+00:00
- `19:17:32`   cot/history/CT.json                                  21,715b  2026-05-01T19:00:47+00:00
- `19:17:32`   cot/history/DX.json                                  19,164b  2026-05-01T19:00:42+00:00
- `19:17:32`   cot/history/ES.json                                  19,695b  2026-05-01T19:00:38+00:00
- `19:17:32`   cot/history/GC.json                                  21,661b  2026-05-01T19:00:44+00:00
- `19:17:32`   cot/history/HG.json                                  21,563b  2026-05-01T19:00:45+00:00
- `19:17:32`   cot/history/HO.json                                  21,550b  2026-05-01T19:00:44+00:00
- `19:17:32`   cot/history/KC.json                                  21,568b  2026-05-01T19:00:49+00:00
- `19:17:32`   cot/history/NG.json                                  22,128b  2026-05-01T19:00:43+00:00
- `19:17:32`   cot/history/NQ.json                                  19,434b  2026-05-01T19:00:38+00:00
- `19:17:32`   cot/history/PL.json                                  21,275b  2026-05-01T19:00:45+00:00
- `19:17:32`   cot/history/RB.json                                  21,505b  2026-05-01T19:00:43+00:00
- `19:17:32`   cot/history/RTY.json                                 19,437b  2026-05-01T19:00:39+00:00
- `19:17:32`   cot/history/SB.json                                  21,808b  2026-05-01T19:00:49+00:00
# 3) justhodl-cot-extremes-scanner config

- `19:17:32`   state: Active
- `19:17:32`   last modified: 2026-04-25T16:11:09.164+0000
# 4) Test invoke cot-extremes-scanner to see where it writes

- `19:17:45`   status: 200
- `19:17:45`   resp: {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"n_processed\": 29, \"n_extreme\": 1, \"n_cluster_alerts\": 0, \"top_extremes\": [{\"contract\": \"HG\", \"pct\": 98.5, \"extreme\": \"high\"}]}"}
# 5) S3 search: any *eurodollar* or *stress* keys

# 6) justhodl-eurodollar-stress config

- `19:17:45`   ✗ An error occurred (ResourceNotFoundException) when calling the GetFunctionConfiguration operation: Function not found: arn:aws:lambda:us-east-1:857687956942:function:justhodl-eurodollar-stress
# 7) Test invoke eurodollar-stress

- `19:17:45`   ✗ An error occurred (ResourceNotFoundException) when calling the Invoke operation: Function not found: arn:aws:lambda:us-east-1:857687956942:function:justhodl-eurodollar-stress:$LATEST
