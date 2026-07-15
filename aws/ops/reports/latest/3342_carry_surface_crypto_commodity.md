## Deploy carry-surface v1.2.0

**Status:** success  
**Duration:** 44.1s  
**Finished:** 2026-07-15T15:55:15+00:00  

## Log
- `15:54:31`   zip: 87034 bytes
## 1. Lambda

- `15:54:31`   Lambda exists — updating
- `15:54:39` ✅   ✓ updated justhodl-carry-surface
## 2. EB rule + permissions

- `15:54:39`   rule already correct: carry-surface-4h (rate(4 hours))
- `15:54:39` ✅   ✓ target → justhodl-carry-surface
- `15:54:39` ✅   ✓ added invoke permission
## 3. Smoke test

- `15:54:39`   invoking justhodl-carry-surface…
- `15:54:55` ✅   ✓ smoke test passed
- `15:54:55`     ok                       True
- `15:54:55`     n_assets                 89
- `15:54:55`     top1                     BRLUSD
- `15:54:55`     top1_carry_pct           8.34
- `15:54:55`     bottom1                  VXX
- `15:54:55`     bottom1_carry_pct        -65.0
- `15:54:55`     telegram_sent            False
- `15:54:55`     elapsed_s                14.26
- `15:54:55` smoke: {"ok": true, "n_assets": 89, "top1": "BRLUSD", "top1_carry_pct": 8.34, "bottom1": "VXX", "bottom1_carry_pct": -65.0, "telegram_sent": false, "elapsed_s": 14.26}
## Verify: invoke + inspect

- `15:55:12` invoke status=200 err=None
- `15:55:15` version=1.2.0 n_assets=89
- `15:55:15` crypto live: 10 -> [('SOL-PERP', 0.1), ('ADA-PERP', -0.6), ('XRP-PERP', -2.9), ('ETH-PERP', -3.1), ('BTC-PERP', -3.4)]
- `15:55:15` commodity: [('USO', 4.0), ('GLD', 0.0), ('SLV', -1.0), ('DBA', -3.0), ('DBC', -3.0), ('UNG', -28.75), ('VXX', -65.0)]
- `15:55:15` ✅ CRYPTO REVIVED — 10 perps live via OKX (was 0).
- `15:55:15` ✅ COMMODITY WINSORIZED — no |carry| > 70 (max=65.0).
- `15:55:15` ✅ version 1.2.0 live
