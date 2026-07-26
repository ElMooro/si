# ops 3923 — vault v2.0 full coverage + gate v2.2 phase-2 wires

**Status:** failure  
**Duration:** 148.4s  
**Finished:** 2026-07-26T21:15:40+00:00  

## Error

```
SystemExit: 1
```

## Data

| brain_text_added | coverage_pct | gate_composite | gate_posture | n_live | n_symbols | statuses |
|---|---|---|---|---|---|---|
| 43 | 66.8 |  |  | 379 | 567 | {'NO_FREE_SOURCE': 186, 'LIVE': 379, 'DISCONTINUED': 2} |
|  |  | -0.47 | RISK_OFF |  |  |  |

## Log
- `21:13:43` ✅   justhodl-tradingview settled: 3
- `21:13:43` ✅   justhodl-risk-gate settled: 1
## vault v2.0 invoke (long: FRED aliases + Yahoo ladder)

- `21:15:32`   CL1!: LIVE value=89.31 src=yahoo:CL=F
- `21:15:32`   MOVE: LIVE value=76.8175 src=yahoo:^MOVE
- `21:15:32`   SPX: LIVE value=7411.98 src=yahoo:^GSPC
- `21:15:32`   UNEMPLOY: LIVE value=7094.0 src=fred
- `21:15:32`   GE1!: DISCONTINUED value=None src=cme
- `21:15:32`   JPLG: NO_FREE_SOURCE value=None src=unresolved_economics
## gate v2.2 invoke — vault wires live

- `21:15:40`   move_index: OK value=76.8175 adj=0.0
- `21:15:40`   oil_backwardation: OK value=5.84 adj=0.0
- `21:15:40` ✅   justhodl-tradingview settled
- `21:15:40` ✅   justhodl-risk-gate settled
- `21:15:40` ✅   zero bare UNRESOLVED (every symbol has a definitive state)
- `21:15:40` ✗   LIVE >= 400
- `21:15:40` ✅   registry grew via brain-text scan
- `21:15:40` ✅   CL1! = LIVE
- `21:15:40` ✅   MOVE = LIVE
- `21:15:40` ✅   SPX = LIVE
- `21:15:40` ✅   UNEMPLOY = LIVE
- `21:15:40` ✅   GE1! = DISCONTINUED
- `21:15:40` ✅   JPLG = NO_FREE_SOURCE
- `21:15:40` ✅   gate consumes vault MOVE
- `21:15:40` ✅   gate oil backwardation input present
- `21:15:40` ✗ FAILED: ['LIVE >= 400']
