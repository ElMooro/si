# ops 6556 - governor v1.2.0 on the $600 cap: verify, sentinel live run, retire the -500 budget

**Status:** success  
**Duration:** 17.0s  
**Finished:** 2026-10-20T18:43:45+00:00  

## Data

| budget_actual | budget_limit | budgets | cadence_checked | cadence_errors | cadence_fixed | cadence_violators | cap | cap_source | held | mode | stage | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2803.622 | 600.0 |  |  |  |  |  | 600.0 | budget | {"FREEZE/lambda": 914, "FREEZE/smasync": 1} | enforce | FREEZE | 1.2.0 |
|  |  |  | 827 | 0 | 0 | 0 |  |  |  |  |  |  |
|  |  | JustHodl-Daily-Spend-Guard, JustHodl-Monthly-Hard-Cap-600 |  |  |  |  |  |  |  |  |  |  |

## Log
- `18:43:30` thresholds now (October override): {"cap": 600.0, "mtd": 2803.62, "freeze_at": 489.32, "brake_at": 440.4, "baseline_usd_day": 24.46, "residual_usd_day": 6.79, "days_left": 11.22}
- `18:43:30` November on $600 at today's baseline $24.46/day, residual $6.79/day: brake at $313, freeze at $362, projected month $758
- `18:43:30`   if November runs at $10/day: brake $450, freeze $490, month ~$310
- `18:43:30`   if November runs at $14/day: brake $446, freeze $486, month ~$434
- `18:43:30`   if November runs at $18/day: brake $442, freeze $482, month ~$558
- `18:43:30` ✅ governor v1.2.0 reads JustHodl-Monthly-Hard-Cap-600 (limit $600), mode enforce, stage FREEZE
- `18:43:34` cadence sentinel: 827 enabled triggers, 0 can fire more than once a day, 0 retimed, 0 errors
- `18:43:34` ✅ cadence sentinel walked 827 enabled triggers live: 0 faster than daily
- `18:43:44` fuse_check: {"checked": true, "firing": [], "runaway": [], "new": [], "suspects": []}
- `18:43:45` ✅ retired JustHodl-Monthly-Hard-Cap-500 (the governor and all five alerts now live on JustHodl-Monthly-Hard-Cap-600)
- `18:43:45` ✅ $600 cap live from 2026-11-01 00:07 UTC (October stays frozen under the $3,500 override)
