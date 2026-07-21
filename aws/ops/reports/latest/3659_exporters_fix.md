# ops 3659 — freight pulse + exporters pulse

**Status:** success  
**Duration:** 56.8s  
**Finished:** 2026-07-21T18:30:22+00:00  

## Error

```
SystemExit: 0
```

## Log
- `18:29:26`   zip: 84442 bytes
## 1. Lambda

- `18:29:26`   Lambda exists — updating
- `18:29:31` ✅   ✓ updated justhodl-freight-pulse
## 3. Smoke test

- `18:29:31`   invoking justhodl-freight-pulse…
- `18:29:36` G1_freight True
- `18:29:37`   zip: 87034 bytes
## 1. Lambda

- `18:29:37`   Lambda exists — updating
- `18:29:42` ✅   ✓ updated justhodl-portwatch
## 3. Smoke test

- `18:29:42`   invoking justhodl-portwatch…
- `18:30:00` G2_exporters False
- `18:30:00`   zip: 107416 bytes
## 1. Lambda

- `18:30:00`   Lambda exists — updating
- `18:30:06` ✅   ✓ updated justhodl-morning-intelligence
## 3. Smoke test

- `18:30:06`   invoking justhodl-morning-intelligence…
- `18:30:22` ✅   ✓ smoke test passed
- `18:30:22`     success                  True
- `18:30:22`     khalid_adj               46.0
- `18:30:22`     regime                   NEUTRAL
- `18:30:22`     btc                      66797
- `18:30:22`     outcomes                 17453
- `18:30:22`     improved                 False
- `18:30:22`     weights_active           277
- `18:30:22`     ka_adj                   46.0
- `18:30:22` G3_serves True
- `18:30:22` VERDICT: GAPS: G2_exporters
