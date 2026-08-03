# ops 4336 -- the fleet grades the fleet (ddb ledger)

**Status:** failure  
**Duration:** 10.0s  
**Finished:** 2026-08-03T21:40:12+00:00  

## Error

```
SystemExit: 1
```

## Log
- `21:40:11` ledger items scanned: 30277
- `21:40:11` attr frequency (top): [('signal_id', 400), ('logged_at', 400), ('baseline_price', 400), ('metadata', 398), ('check_windows', 396), ('horizon_days_primary', 377), ('signal_type', 260), ('status', 260), ('ttl', 260), ('logged_epoch', 260), ('predicted_direction', 258), ('signal_value', 258), ('outcomes', 258), ('measure_against', 256), ('confidence', 248), ('check_timestamps', 248)]
- `21:40:11` status sample dist: {'None': 140, 'partial': 101, 'complete': 114, 'pending': 42, 'unscoreable': 3}
- `21:40:11` sample item: {"benchmark": "None", "signal_type": "eng:strategist", "metadata": "{'trust_weight': Decimal('0'), 'raw_score': None, 'engine': ", "logged_at": "2026-07-15T23:15:30.834183+00:00", "check_windows": "['7', '14', '30']", "horizon_days_primary": "30", "predicted_direction": "UP", "regime_at_log": "SLOWING", "status": "partial", "ttl": "1815693330", "predicted_magnitude_pct": "None", "supporting_signals": "None", "last_checked": "2026-07-30T21:29:02.987115+00:00", "logged_epoch": "1784157330", "signal_value": "PICK", "c
- `21:40:11` graded items usable: 0 across 0 engines
## TOP SUCCESS ENGINES

## TOP FAILURE ENGINES

- `21:40:12` ✗ no graded items with n>=5 -- schema printed above
