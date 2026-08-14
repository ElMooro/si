# ops 4660 — depth ledger: state -> catalog -> card

**Status:** failure  
**Duration:** 844.3s  
**Finished:** 2026-08-14T17:47:17+00:00  

## Error

```
SystemExit: 1
```

## Log
- `17:33:13` fred guard (untouched): imported=281947 status=COMPLETE_WITH_LEAKS
## 1. Settle deploys, kick nyfed

- `17:33:13`   nyfed settled=True catalog settled=True
- `17:33:13`   before: done=223 depth=None
- `17:42:15`   after 542s: done=305 depth={'keys': 305, 'n_obs_sum': 69621, 'first_min': '2013-04-03', 'multi': 305, 'ge500': 81}
- `17:42:15` ✅   [ledger] depth ledger present (keys=305)
- `17:42:15` ✅   [ledger] ledger keys 305 == done 305 (backfill + incremental consistent)
- `17:42:15` ✅   [ledger] mean n_obs 228 (shallow era ~110)
- `17:42:15` ✅   [ledger] multi-break 305/305
- `17:42:15` ✅   [ledger] earliest first 2013-04-03 (full lineage reached)
## 2. Kick provider-catalog, contract the card payload

- `17:47:17`   card payload depth: {}
- `17:47:17` ✗   [card] CONTRACT MISS — depth reached the provider payload (keys=None)
- `17:47:17`   convergence: 305 done, 1234 remaining ≈ 9 h
## verdict

- `17:47:17` ✗ depth ledger: 1 red
