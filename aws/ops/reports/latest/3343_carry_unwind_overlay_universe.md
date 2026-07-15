## Deploy carry-surface v1.3.0

**Status:** success  
**Duration:** 59.0s  
**Finished:** 2026-07-15T16:04:21+00:00  

## Log
- `16:03:22`   zip: 90045 bytes
## 1. Lambda

- `16:03:22`   Lambda exists — updating
- `16:03:25` ✅   ✓ updated justhodl-carry-surface
## 2. EB rule + permissions

- `16:03:26`   rule already correct: carry-surface-4h (rate(4 hours))
- `16:03:26` ✅   ✓ target → justhodl-carry-surface
- `16:03:26` ✅   ✓ added invoke permission
## 3. Smoke test

- `16:03:26`   invoking justhodl-carry-surface…
- `16:03:51` ✅   ✓ smoke test passed
- `16:03:51`     ok                       True
- `16:03:51`     n_assets                 166
- `16:03:51`     top1                     TURUSD
- `16:03:51`     top1_carry_pct           13.03
- `16:03:51`     bottom1                  VXX
- `16:03:51`     bottom1_carry_pct        -65.0
- `16:03:51`     telegram_sent            False
- `16:03:51`     elapsed_s                24.15
- `16:03:51` smoke: {"ok": true, "n_assets": 166, "top1": "TURUSD", "top1_carry_pct": 13.03, "bottom1": "VXX", "bottom1_carry_pct": -65.0, "telegram_sent": false, "elapsed_s": 24.15}
## Verify: invoke + inspect

- `16:04:18` invoke status=200 err=None
- `16:04:21` version=1.3.0 n_assets=166 by_class={'fx': 16, 'fixed_income': 17, 'equity': 109, 'commodity': 14, 'crypto': 10}
- `16:04:21` UNWIND: cohort_fragility=44.1 verdict=LOW — carry cohort is not currently stretched
- `16:04:21`   regime=None roro=None vix=None mult=1.0
- `16:04:21`   n_fragile=0 n_crowded=4
- `16:04:21`   top fragile: []
- `16:04:21` ✅ UNWIND OVERLAY LIVE — cohort=44.1, 0 fragile / 4 crowded.
- `16:04:21` ✅ risk-regime joined: None (RORO None, VIX None).
- `16:04:21` ✅ UNIVERSE EXPANDED — 166 assets ({'fx': 16, 'fixed_income': 17, 'equity': 109, 'commodity': 14, 'crypto': 10}).
- `16:04:21` ✅ no regressions — crypto=10, max commodity |carry|=65.0.
- `16:04:21` ✅ version 1.3.0 live
