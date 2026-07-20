# ops 3583 — asia-leads v1 (China TSF · KR/TW exports · US calendar)

**Status:** success  
**Duration:** 30.5s  
**Finished:** 2026-07-20T18:24:17+00:00  

## Error

```
SystemExit: 0
```

## Log
- `18:23:47`   zip: 85079 bytes
## 1. Lambda

- `18:23:47`   Lambda missing — creating
- `18:23:50` ✅   ✓ created justhodl-asia-leads
## 3. Smoke test

- `18:23:50`   invoking justhodl-asia-leads…
- `18:24:15` ✅   ✓ smoke test passed
- `18:24:15`     ok                       True
- `18:24:15`     tsf_series               8
- `18:24:15`     kr_yoy                   47.96
- `18:24:15`     tw_yoy                   48.33
- `18:24:15`     cal_high_impact          20
- `18:24:15` PASS  G1_deployed — deployed via ops helper (no workflow stomp)
- `18:24:16` PASS  G2_schedule — created justhodl-asia-leads-daily cron(20 10 * * ? *)
- `18:24:17` PASS  G3_feed_real — tsf_series=8 headline=[('2025', 356000.0), ('2024', 170496.0)] kr_yoy=47.96% (2026-04-01) tw_yoy=48.33% (2026-05-01) cal_hi=20 next_hi=[('2026-07-20', 'FOMC Press Release'), ('2026-07-21', 'FOMC Press Release'), ('2026-07-22', 'FOMC Press Release'), ('2026-07-23', 'FOMC Press Release')]
- `18:24:17` VERDICT: PASS_ALL
