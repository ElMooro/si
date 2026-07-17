# ops 3403 — composer cap + best-setups wires

**Status:** success  
**Duration:** 259.0s  
**Finished:** 2026-07-17T21:35:38+00:00  

## Error

```
SystemExit: 0
```

## Log
- `21:31:46` PASS  G1_deployed — composer 1.1 + best-setups self-log markers
- `21:35:16` FAIL  G2_composer_capped — book=0 gross=0% mode=None top=[]
- `21:35:38` FAIL  G3_wires_live — rows_fielded=True stack_signals_today=0 sample=[{'ticker': 'BKNG', 'rel_volume': 0.86, 'entry_confirmed': False, 'hold_horizon_days': 21}, {'ticker': 'NVDA', 'rel_volume': None, 'entry_confirmed': None, 'hold_horizon_days': 21}, {'ticker': 'ABBV', 'rel_volume': 1.12, 'entry_confirmed': Fal
- `21:35:38` VERDICT: GAPS: G2_composer_capped,G3_wires_live
