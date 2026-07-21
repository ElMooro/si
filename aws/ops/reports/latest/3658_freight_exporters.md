# ops 3658 — freight pulse + exporters pulse

**Status:** success  
**Duration:** 170.6s  
**Finished:** 2026-07-21T18:26:50+00:00  

## Error

```
SystemExit: 0
```

## Log
- `18:23:59`   zip: 84442 bytes
## 1. Lambda

- `18:24:00`   Lambda exists — updating
- `18:24:04` ✅   ✓ updated justhodl-freight-pulse
## 3. Smoke test

- `18:24:04`   invoking justhodl-freight-pulse…
- `18:24:12` G1_freight True
- `18:24:12`   zip: 86997 bytes
## 1. Lambda

- `18:24:12`   Lambda exists — updating
- `18:24:17` ✅   ✓ updated justhodl-portwatch
## 3. Smoke test

- `18:24:17`   invoking justhodl-portwatch…
- `18:24:37` G2_exporters False
- `18:24:37`   zip: 107416 bytes
## 1. Lambda

- `18:24:37`   Lambda exists — updating
- `18:24:40` ✅   ✓ updated justhodl-morning-intelligence
## 3. Smoke test

- `18:24:40`   invoking justhodl-morning-intelligence…
- `18:25:09` ✅   ✓ smoke test passed
- `18:25:09`     success                  True
- `18:25:09`     khalid_adj               46.0
- `18:25:09`     regime                   NEUTRAL
- `18:25:09`     btc                      66797
- `18:25:09`     outcomes                 17453
- `18:25:09`     improved                 False
- `18:25:09`     weights_active           277
- `18:25:09`     ka_adj                   46.0
- `18:26:50` G3_serves True
- `18:26:50` VERDICT: GAPS: G2_exporters
