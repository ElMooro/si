# ops 3600 — barometer top placement + 1990→today spillover history

**Status:** success  
**Duration:** 140.8s  
**Finished:** 2026-07-20T22:11:47+00:00  

## Error

```
SystemExit: 0
```

## Log
- `22:09:26`   zip: 86967 bytes
## 1. Lambda

- `22:09:26`   Lambda exists — updating
- `22:09:30` ✅   ✓ updated justhodl-fifx-vol-migration
## 3. Smoke test

- `22:09:30`   invoking justhodl-fifx-vol-migration…
- `22:09:47` ✅   ✓ smoke test passed
- `22:09:47`     ok                       True
- `22:09:47`     state                    CALM
- `22:09:47`     spillover                -0.77
- `22:09:47`     fi_z                     -0.84
- `22:09:47`     fx_z                     -0.76
- `22:09:47`     eq_z                     0.01
- `22:10:00` PASS  G1_deep_shard — rows=2212 span 1990-03-30→2026-07-17 · crisis spill maxima: LTCM97-98=4.09 GFC08-09=2.88 COVID20=2.96 · last={'d': '2026-07-17', 'fis': -0.49, 'fx': -0.76, 'eq': 0.01, 'spill': -0.5}
- `22:10:01` PASS  G2_main_intact — main v1.2.0 hist=173 state=CALM spill=-0.77
- `22:11:47` PASS  G3_page_top_deep — order_top=True deep_markers=True
- `22:11:47` VERDICT: PASS_ALL
