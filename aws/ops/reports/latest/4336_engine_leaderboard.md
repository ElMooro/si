# ops 4336 -- the fleet grades the fleet (ddb ledger)

**Status:** success  
**Duration:** 9.3s  
**Finished:** 2026-08-03T21:50:36+00:00  

## Log
- `21:50:36` ledger items scanned: 30283
- `21:50:36` attr frequency (top): [('signal_id', 400), ('logged_at', 400), ('baseline_price', 400), ('metadata', 398), ('check_windows', 396), ('horizon_days_primary', 377), ('signal_type', 260), ('status', 260), ('ttl', 260), ('logged_epoch', 260), ('predicted_direction', 258), ('signal_value', 258), ('outcomes', 258), ('measure_against', 256), ('confidence', 248), ('check_timestamps', 248)]
- `21:50:36` status sample dist: {'None': 140, 'partial': 101, 'complete': 114, 'pending': 42, 'unscoreable': 3}
- `21:50:36` sample item: {"benchmark": "None", "signal_type": "eng:strategist", "metadata": "{'trust_weight': Decimal('0'), 'raw_score': None, 'engine': ", "logged_at": "2026-07-15T23:15:30.834183+00:00", "check_windows": "['7', '14', '30']", "horizon_days_primary": "30", "predicted_direction": "UP", "regime_at_log": "SLOWING", "status": "partial", "ttl": "1815693330", "predicted_magnitude_pct": "None", "supporting_signals": "None", "last_checked": "2026-07-30T21:29:02.987115+00:00", "logged_epoch": "1784157330", "signal_value": "PICK", "c
- `21:50:36` outcomes sample [strategist/partial]: {"day_14": {"correct": false, "price_at_check": "333.66", "checked_at": "2026-07-30T21:29:02.987115+00:00", "regime_at_log": "UNKNOWN", "return_pct": "-10.045293", "actual_direction": "DOWN", "price_at_signal": "370.92"}, "day_7": {"return_pct": "-14.350803", "price_at_check": "317.69", "regime_at_log": "UNKNOWN", "correct": false, "price_at_signal": "370.92", "actual_direction": "DOWN", "checked_at": "2026-07-23T21:
- `21:50:36` outcomes sample [liquidity-capacity/complete]: {"day_30": {"actual_direction": "UP", "checked_at": "2026-07-31T21:29:03.248760+00:00", "price_at_check": "11.7", "regime_at_log": "UNKNOWN", "price_at_signal": "11.13", "correct": true, "return_pct": "5.121294"}, "day_14": {"price_at_signal": "11.13", "checked_at": "2026-07-15T21:29:02.993890+00:00", "actual_direction": "UP", "price_at_check": "11.4", "correct": true, "return_pct": "2.425876"}, "day_7": {"checked_at
- `21:50:36` graded items usable: 17115 across 333 engines
## TOP SUCCESS ENGINES

- `21:50:36` crypto_options_rr              100.0% · n=8    avg=3.16 · https://justhodl-dashboard-live.s3.amazonaws.com/data/crypto_options_rr.json
- `21:50:36` crypto_cot_assetmgr            100.0% · n=5    avg=5.65 · https://justhodl-dashboard-live.s3.amazonaws.com/data/crypto_cot_assetmgr.json
- `21:50:36` insider-sell-cluster           100.0% · n=5    avg=13.45 · https://justhodl.ai/insiders.html
- `21:50:36` crisis_dfii10_vs_gld            92.4% · n=79   avg=-8.46 · https://justhodl-dashboard-live.s3.amazonaws.com/data/crisis_dfii10_vs_gld.json
- `21:50:36` auction-decisive-call           88.9% · n=9    avg=134110.58 · https://justhodl-dashboard-live.s3.amazonaws.com/data/auction-decisive-call.json
- `21:50:36` rating-change-cluster           85.7% · n=7    avg=3.26 · https://justhodl-dashboard-live.s3.amazonaws.com/data/rating-change-cluster.json
- `21:50:36` capital-return                  85.2% · n=27   avg=7.99 · https://justhodl-dashboard-live.s3.amazonaws.com/data/capital-return.json
- `21:50:36` political-intel                 80.0% · n=30   avg=2.39 · https://justhodl-dashboard-live.s3.amazonaws.com/data/political-intel.json
- `21:50:36` attention_distribution          78.6% · n=98   avg=-10.36 · https://justhodl-dashboard-live.s3.amazonaws.com/data/attention_distribution.json
- `21:50:36` gf-value                        78.1% · n=32   avg=3.93 · https://justhodl-dashboard-live.s3.amazonaws.com/data/gf-value.json
## TOP FAILURE ENGINES

- `21:50:36` crypto-ma200                     0.0% · n=5    avg=-0.68 · https://justhodl-dashboard-live.s3.amazonaws.com/data/crypto-ma200.json
- `21:50:36` nobrainer_SIL                    0.0% · n=6    avg=-24.96 · https://justhodl-dashboard-live.s3.amazonaws.com/data/nobrainer_sil.json
- `21:50:36` nobrainer_PICK                   0.0% · n=7    avg=-11.32 · https://justhodl-dashboard-live.s3.amazonaws.com/data/nobrainer_pick.json
- `21:50:36` btc_mvrv                         0.0% · n=8    avg=3.44 · https://justhodl-dashboard-live.s3.amazonaws.com/data/btc_mvrv.json
- `21:50:36` onchain_composite_risk           0.0% · n=12   avg=1.43 · https://justhodl.ai/risk-gate.html
- `21:50:36` sector_breadth                   0.0% · n=12   avg=2.47 · https://justhodl-dashboard-live.s3.amazonaws.com/data/sector_breadth.json
- `21:50:36` bonds-decisive-call              0.0% · n=17   avg=-1.54 · https://justhodl-dashboard-live.s3.amazonaws.com/data/bonds-decisive-call.json
- `21:50:36` asset-compass                    0.0% · n=18   avg=-0.51 · https://justhodl-dashboard-live.s3.amazonaws.com/data/asset-compass.json
- `21:50:36` cot_extreme                      0.0% · n=45   avg=-0.2 · https://justhodl-dashboard-live.s3.amazonaws.com/data/cot_extreme.json
- `21:50:36` wl-engines                       0.0% · n=50   avg=-1.27 · https://justhodl-dashboard-live.s3.amazonaws.com/data/wl-engines.json
- `21:50:36` ✅ leaderboard LIVE: data/engine-leaderboard.json (300 engines)
