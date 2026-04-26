# Verify calibrator fix — ka_* aliases in S3 after SSM-ordering fix

**Status:** success  
**Duration:** 6.8s  
**Finished:** 2026-04-26T13:05:19+00:00  

## Log
## A. Confirm deployed code has SSM-puts-before-aliasing

- `13:05:13`   SSM put positions: [12831, 12972, 13583]
- `13:05:13`   add_ka_aliases positions: [13946]
- `13:05:13`   S3 put positions: [14051, 14256]
- `13:05:13`   ✅ ordering correct: SSM (13583) → alias (13946) → S3 (14051)
## B. Force-invoke calibrator

- `13:05:14`   ✅ 1.3s err=none
- `13:05:14`   payload preview: {"statusCode": 200, "body": "{\"success\": true, \"total_outcomes\": 40, \"weights_updated\": {\"crypto_fear_greed\": 1.11, \"crypto_risk_score\": 0.35, \"momentum_uso\": 0.35, \"edge_composite\": 1.34, \"plumbing_stress\": 1.34, \"khalid_index\": 1.0, \"screener_top_pick\": 0.85, \"valuation_compos
## C. Verify calibration/latest.json now has ka_* aliases

- `13:05:19`   size: 5459B  age: 4.9s
- `13:05:19`   khalid_* keys (2): ['khalid_component_weights', 'khalid_index']
- `13:05:19`   ka_* keys     (2): ['ka_component_weights', 'ka_index']
- `13:05:19`   ▸ ✅ DUAL-WRITE-OK — all 2 aliased
- `13:05:19` Done
