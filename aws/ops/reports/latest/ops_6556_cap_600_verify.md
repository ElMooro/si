# ops 6556 - governor v1.2.0 on the $600 cap: verify, sentinel live run, retire the -500 budget

**Status:** failure  
**Duration:** 1.8s  
**Finished:** 2026-10-20T18:37:57+00:00  

## Error

```
SystemExit: 1
```

## Data

| budget_actual | budget_limit | cap | cap_source | held | mode | stage | version |
|---|---|---|---|---|---|---|---|
| 2803.622 | 500.0 | 500.0 | budget | {"FREEZE/lambda": 914, "FREEZE/smasync": 1} | enforce | FREEZE | 1.1.1 |

## Log
- `18:37:57` thresholds now (October override): {"cap": 500.0, "mtd": 2803.62, "freeze_at": 389.29, "brake_at": 340.38, "baseline_usd_day": 24.46, "residual_usd_day": 6.79, "days_left": 11.22}
- `18:37:57` November on $600 at today's baseline $24.46/day, residual $6.79/day: brake at $313, freeze at $362, projected month $758
- `18:37:57`   if November runs at $10/day: brake $450, freeze $490, month ~$310
- `18:37:57`   if November runs at $14/day: brake $446, freeze $486, month ~$434
- `18:37:57`   if November runs at $18/day: brake $442, freeze $482, month ~$558
- `18:37:57` ✗ status does not show v1.2.0 on the $600 budget in enforce mode
