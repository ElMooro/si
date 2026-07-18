- `03:24:56` ")
- `03:24:56` 
**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-07-18T03:24:56+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:24:56`             # Also write to outcomes table for easy aggregation
- `03:24:56`             outcomes_table.put_item(Item=float_to_decimal({
- `03:24:56`                 "outcome_id":    f"{signal_id}_{window_key}",
- `03:24:56`                 "signal_id":     signal_id,
- `03:24:56`                 "signal_type":   signal_type,
- `03:24:56`                 "signal_value":  signal.get("signal_value"),
- `03:24:56`                 "window_key":    window_key,
- `03:24:56`                 "regime_at_log": outcome.get("regime_at_log", "UNKNOWN"),
- `03:24:56`                 "correct":       correct,
- `03:24:56`                 "predicted_dir": pred_dir,
- `03:24:56`                 "outcome":       outcome,
- `03:24:56`                 "logged_at":     signal.get("logged_at"),
- `03:24:56`                 "regime_at_log": signal.get("regime_at_log"),
- `03:24:56`                 "checked_at":    now_iso,
- `03:24:56`                 "ttl":         
- `03:24:56` REGIME_BRIDGE_V1 (ops 3442): regime AT LOG TIME rides on every
- `03:24:56`             # outcome so scorecard.by_regime / engine-trust conditioning is fed.
- `03:24:56`             try:
- `03:24:56`                 _mr = (signal.get("metadata") or {}).get("regime") or {}
- `03:24:56`                 _lbl = (_mr.get("label")
- `03:24:56`                         or (f"J{_mr.get('jsi_decile')}|{_mr.get('gssi_band')}"
- `03:24:56`                             if _mr.get("jsi_decile") is not None
- `03:24:56`                             or _mr.get("gssi_band") else None))
- `03:24:56`                 o
