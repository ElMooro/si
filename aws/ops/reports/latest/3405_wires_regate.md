# ops 3403 — composer cap + best-setups wires

**Status:** success  
**Duration:** 39.2s  
**Finished:** 2026-07-17T21:43:14+00:00  

## Error

```
SystemExit: 0
```

## Log
- `21:42:36` PASS  G1_deployed — composer 1.1 + best-setups self-log markers
- `21:42:53` PASS  G2_composer_capped — book=40 gross=100.0% mode=PROVEN top=['MSFT', 'GILD', 'FOX', 'WTFC', 'META', 'FOXA']
- `21:43:14` FAIL  G3_wires_live — rows_fielded=True stack_signals_today=0 sample=[{'ticker': 'NVDA', 'rel_volume': None, 'entry_confirmed': None, 'hold_horizon_days': 21}, {'ticker': 'BKNG', 'rel_volume': 0.86, 'entry_confirmed': False, 'hold_horizon_days': 21}, {'ticker': 'ABBV', 'rel_volume': 1.12, 'entry_confirmed': Fal
- `21:43:14` VERDICT: GAPS: G3_wires_live
