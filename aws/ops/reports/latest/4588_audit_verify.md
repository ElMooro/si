# ops 4588 — already-built audit convergence

**Status:** failure  
**Duration:** 15.9s  
**Finished:** 2026-08-10T22:23:38+00:00  

## Error

```
SystemExit: 1
```

## Log
- `22:23:22`   fired impact-graph
- `22:23:38`   graph refreshed (15s)
## 1. Coverage above the census ceiling

- `22:23:38` ✅   [graph] industry coverage 797 (was 502; reused=0 filled_tonight=295 still_missing_top_adv=11197)
- `22:23:38` ✅   [graph] audit trail on the payload
## 2. Convergence consumes the prior-art engine

- `22:23:38` ✗   [convergence] CONTRACT MISS — relationship note names the authority
- `22:23:38` ✗   [convergence] CONTRACT MISS — flow_confluence votes present (source engine has 40 multi-engine names; voted=False)
- `22:23:38`     Banks - Regional ACCUMULATION score=5.0 sources=['flow_lookthrough', 'dark_pool']
- `22:23:38`     Banks - Diversified ACCUMULATION score=4.0 sources=['flow_lookthrough', 'dark_pool']
- `22:23:38`     Travel Services ACCUMULATION score=2.0 sources=['flow_lookthrough', 'dark_pool']
- `22:23:38`     Asset Management ACCUMULATION score=2.0 sources=['flow_lookthrough', 'dark_pool']
- `22:23:38`     Integrated Freight & Logistics DISTRIBUTION score=0.0 sources=['flow_lookthrough', 'dark_pool']
## verdict

- `22:23:38` ✗ audit convergence: 2 red
