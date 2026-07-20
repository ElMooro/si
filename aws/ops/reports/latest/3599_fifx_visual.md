# ops 3599 — vol barometer v2 (gauge + flow + ribbon)

**Status:** success  
**Duration:** 119.0s  
**Finished:** 2026-07-20T21:59:58+00:00  

## Error

```
SystemExit: 0
```

## Log
- `21:57:59`   zip: 86318 bytes
## 1. Lambda

- `21:57:59`   Lambda exists — updating
- `21:58:07` ✅   ✓ updated justhodl-fifx-vol-migration
## 3. Smoke test

- `21:58:07`   invoking justhodl-fifx-vol-migration…
- `21:58:09` ✅   ✓ smoke test passed
- `21:58:09`     ok                       True
- `21:58:09`     state                    CALM
- `21:58:09`     spillover                -0.77
- `21:58:09`     fi_z                     -0.84
- `21:58:09`     fx_z                     -0.76
- `21:58:09`     eq_z                     0.01
- `21:58:12` PASS  G1_history_real — v1.1.0 hist=173 first=2025-11-05 last=2026-07-17 spill_last=-0.77 vs headline -0.77 state=CALM fi_z=-0.94 fx_z=-0.76 eq_z=0.01
- `21:59:58` PASS  G2_card_served — served: gauge + migration map + ribbon + flow animation
- `21:59:58` VERDICT: PASS_ALL
