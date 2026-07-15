## ENSURE FMP_KEY

**Status:** success  
**Duration:** 7.7s  
**Finished:** 2026-07-15T01:54:52+00:00  

## Data

| RESULT | age_seconds | attempt_1 | counts | data_source | engine_had_fmp | fmp_key_suffix | generated_at | n_most_bullish | n_top_picks | sample_bullish | total_signals | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | False | S8xb |  |  |  |  |  |  |
|  |  | {'fn_error': None, 'resp': {'statusCode': 200, 'body': '{"upgrades": 85, "downgrades": 76, "pt_raises": 831, "guidance_raises": 0, "guidance_cuts": 0, "n_picks": 20, "top_pick": "FCEL", "elapsed_s": 0.3}'}, 'written_version': '2.0.0'} |  |  |  |  |  |  |  |  |  |  |
|  |  |  | {'ratings_7d': 1219, 'guidance_21d': 0, 'insights_7d': 0, 'upgrades': 85, 'downgrades': 76, 'pt_raises': 831, 'pt_cuts': 99, 'guidance_raises': 0, 'guidance_cuts': 0} | FMP grades-latest-news + price-target-latest-news (/stable) |  |  | 2026-07-15T01:54:49.508001+00:00 | 30 | 20 | {'ticker': 'FCEL', 'company': 'FCEL', 'net_score': 24.0, 'signals': ['UPGRADE None→Overweight (Stephens)', 'PT RAISE 19.08→27 (Stephens)', 'PT RAISE 19.08→27 (Evercore ISI)', 'UPGRADE Hold→Buy (Cowen & Co.)', 'PT RAISE 19.08→27 (Cowen & Co.)', 'DOWNGRADE None→Reduce (HSBC)', 'PT RAISE 19.08→27 (HSBC)', 'DOWNGRADE Sector Weight→Sector Weight (KeyBanc)', 'PT RAISE 19.08→27 (KeyBanc)', 'PT RAISE 19.08→27 (BMO Capital)', 'UPGRADE None→Overweight (Piper Sandler)', 'PT RAISE 19.08→27 (Piper Sandler)', 'UPGRADE Underweight→Overweight (Wells Fargo)', 'PT RAISE 19.08→27 (Wells Fargo)', 'UPGRADE Neutral→Buy (UBS)', 'PT RAISE 19.08→27 (UBS)'], 'n_up': 5, 'n_down': 2, 'n_pt_raise': 9, 'n_pt_cut': 0, 'n_guid_raise': 0, 'n_guid_cut': 0, 'n_distinct': 3, 'bull_types': 2} | 2310 | 2.0.0 |
|  | 3 |  |  |  |  |  |  |  |  |  |  |  |
| FIXED |  |  |  |  |  |  |  |  |  |  |  |  |

## Log
- `01:54:48` ✅ FMP_KEY set on engine env
## FORCE RUN (await v2.0)

## VERIFY FEED

## VERDICT

- `01:54:52` ✅ analyst-actions v2.0 LIVE on FMP data — page renders real analyst rating transitions + PT moves. Benzinga dependency retired.
