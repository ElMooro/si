# Patch calibrator to filter is_legacy + correct=None

**Status:** success  
**Duration:** 7.8s  
**Finished:** 2026-04-25T20:53:59+00:00  

## Data

| invoke_s | zip_size |
|---|---|
| 1.2 | 15571 |

## Log
## 1. Patch justhodl-calibrator

- `20:53:51`   Source: 14,285B
- `20:53:51` ✅   Patched calibrator filter
- `20:53:51` ✅   Syntax OK
## 2. Check + patch reports-builder

- `20:53:51`   reports-builder doesn\'t scan OUTCOMES_TABLE directly
## 3. Deploy patched calibrator

- `20:53:55` ✅   Deployed calibrator (15,571B, 1 files)
## 4. Test invoke

- `20:53:59` ✅   Invoked in 1.2s
- `20:53:59`   Response: {"statusCode": 200, "body": "{\"success\": true, \"total_outcomes\": 0, \"weights_updated\": {\"khalid_index\": 1.0, \"screener_top_pick\": 0.85, \"valuation_composite\": 0.8, \"cftc_gold\": 0.8, \"cftc_spx\": 0.8, \"cftc_bitcoin\": 0.75, \"cftc_crude\": 0.7, \"edge_regime\": 0.75, \"edge_composite\": 0.7, \"market_phase\": 0.75, \"crypto_btc_signal\": 0.7, \"crypto_eth_signal\": 0.65, \"crypto_fear_greed\": 0.55, \"crypto_risk_score\": 0.55, \"btc_mvrv\": 0.7, \"carry_risk\": 0.65, \"ml_risk\":
- `20:53:59` Done
