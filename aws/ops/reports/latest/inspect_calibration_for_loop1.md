# Loop 1 prep — calibration shape + consumer identification

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-04-25T10:44:40+00:00  

## Log
## 1. SSM calibration parameters

- `10:44:40` 
  /justhodl/calibration/weights
- `10:44:40`     Type: String, length: 284B
- `10:44:40`     Parsed type: dict
- `10:44:40`     Top-level keys (12): ['cftc_bitcoin', 'cftc_crude', 'cftc_gold', 'cftc_spx', 'crypto_btc_signal', 'crypto_eth_signal', 'crypto_fear_greed', 'crypto_risk_score', 'edge_regime', 'khalid_index']
- `10:44:40`     Sample entry: 'cftc_bitcoin' → 0.75
- `10:44:40` 
  /justhodl/calibration/accuracy
- `10:44:40`     Type: String, length: 140B
- `10:44:40`     Parsed type: dict
- `10:44:40`     Top-level keys (2): ['crypto_fear_greed', 'crypto_risk_score']
- `10:44:40`     Sample entry: 'crypto_fear_greed' → {"accuracy": 0.0, "n": 369, "avg_return": null}
- `10:44:40` 
  /justhodl/calibration/report
- `10:44:40`     Type: String, length: 2881B
- `10:44:40`     Parsed type: dict
- `10:44:40`     Top-level keys (10): ['accuracy_by_type', 'generated_at', 'khalid_component_weights', 'recommendations', 'signal_types_tracked', 'top_performing_signals', 'total_outcomes', 'weights', 'window_accuracy', 'worst_performing_signals']
- `10:44:40`     Sample entry: 'accuracy_by_type' → {"crypto_risk_score": {"n": 369, "n_correct": 0, "n_wrong": 0, "n_unknown": 369, "accuracy": 0.0, "up_precision": 0.0, "down_precision": 0.0, "avg_return": null, "positive_returns": 0, "negative_retur
- `10:44:40` ⚠   /justhodl/calibration/last_run: NOT FOUND
## 2. Calibrator source — what shape does it produce?

- `10:44:40`   aws/lambdas/justhodl-calibrator/source/lambda_function.py (393 LOC)
- `10:44:40`     L256: weights[stype] = accuracy_to_weight(acc, stats["n"], default_weight)
- `10:44:40`     L258: weights[stype] = default_weight
- `10:44:40`     L268: weights[stype] = default_w
## 3. Consumer Lambda identification

- `10:44:40` 
  justhodl-intelligence (874 LOC)
- `10:44:40`     Already reads calibration: False
- `10:44:40`     Signal types referenced: ['carry_risk', 'khalid_index']
- `10:44:40`     Aggregation hints (top 3):
- `10:44:40`       • ki_score=ki_raw.get("score", 0) or 0
- `10:44:40`       • edge_score=edge.get("composite_score", 0) if isinstance(edge, dict) else 0
- `10:44:40`       • sector_picks=sorted(sector_picks, key=lambda x: x.get("score", 0), reverse=True)[:5]
- `10:44:40` 
  justhodl-morning-intelligence (358 LOC)
- `10:44:40`     Already reads calibration: True
- `10:44:40`     Signal types referenced: ['khalid_index']
- `10:44:40`     Aggregation hints (top 3):
- `10:44:40`       • scores=intel.get("scores",{})
- `10:44:40` 
  justhodl-edge-engine (189 LOC)
- `10:44:40`     Already reads calibration: False
- `10:44:40`     Signal types referenced: []
- `10:44:40`     Aggregation hints (top 3):
- `10:44:40`       • if vix > 30: score -= 20
- `10:44:40`       • elif vix > 20: score -= 10
- `10:44:40`       • elif vix < 15: score += 15
- `10:44:40` 
  justhodl-daily-report-v3 (1791 LOC)
- `10:44:40`     Already reads calibration: False
- `10:44:40`     Signal types referenced: []
- `10:44:40`     Aggregation hints (top 3):
- `10:44:40`       • score = s.get('score', 50)
- `10:44:40`       • most_bought.sort(key=lambda x: x['flow_score'], reverse=True)
- `10:44:40`       • elif rsi<40: score+=6
- `10:44:40` 
  justhodl-investor-agents (196 LOC)
- `10:44:40`     Already reads calibration: False
- `10:44:40`     Signal types referenced: []
- `10:44:40`     Aggregation hints (top 3):
- `10:44:40`       • scores=safe(raw.get("scores"),{})
- `10:44:40` 
  justhodl-signal-logger (299 LOC)
- `10:44:40`     Already reads calibration: False
- `10:44:40`     Signal types referenced: ['crypto_fear_greed', 'crypto_risk_score', 'edge_composite', 'edge_regime', 'khalid_index', 'market_phase', 'plumbing_stress', 'screener_top_pick']
- `10:44:40`     Aggregation hints (top 3):
- `10:44:40`       • _REGIME_SNAPSHOT["khalid_score"]=int(float(ki.get("score",0))) if ki.get("score") is not None else None
- `10:44:40`       • _REGIME_SNAPSHOT["khalid_score"]=int(float(ki))
- `10:44:40`       • print(f"[REGIME] snapshot: regime={_REGIME_SNAPSHOT['regime']}, score={_REGIME_SNAPSHOT['khalid_score']}")
- `10:44:40` 
  justhodl-outcome-checker (386 LOC)
- `10:44:40`     Already reads calibration: False
- `10:44:40`     Signal types referenced: []
- `10:44:40`     Aggregation hints (top 3):
- `10:44:40`       • def score_directional(predicted_direction, baseline_price, current_price, threshold_pct=0.5):
- `10:44:40` 
  justhodl-calibrator (393 LOC)
- `10:44:40`     Already reads calibration: True
- `10:44:40`     Signal types referenced: ['edge_regime']
## 4. Patch priority

- `10:44:40`   Priority order based on findings:
- `10:44:40`     1. justhodl-intelligence — produces ML risk score (5/5 critical)
- `10:44:40`     2. justhodl-morning-intelligence — composes daily brief
- `10:44:40`     3. justhodl-edge-engine — produces edge composite
- `10:44:40`     4. justhodl-daily-report-v3 — produces khalid_index itself
- `10:44:40`        (special case — its OUTPUT is calibrated, not its inputs)
- `10:44:40` 
  Skip:
- `10:44:40`     - signal-logger (records, doesn't predict)
- `10:44:40`     - outcome-checker (scores, doesn't predict)
- `10:44:40`     - calibrator (produces weights, would create cycle)
- `10:44:40` Done
