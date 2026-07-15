## Deploy carry-surface v1.4.0 (crypto removed)

**Status:** success  
**Duration:** 38.6s  
**Finished:** 2026-07-15T16:50:33+00:00  

## Log
- `16:49:54`   zip: 89044 bytes
## 1. Lambda

- `16:49:54`   Lambda exists — updating
- `16:50:01` ✅   ✓ updated justhodl-carry-surface
## 2. EB rule + permissions

- `16:50:01`   rule already correct: carry-surface-4h (rate(4 hours))
- `16:50:02` ✅   ✓ target → justhodl-carry-surface
- `16:50:02` ✅   ✓ added invoke permission
## Verify: async invoke + poll S3

- `16:50:02` async invoke fired; polling S3…
- `16:50:33` fresh write after ~30s
- `16:50:33` version=1.4.0 n_assets=156 classes={'fx': 16, 'fixed_income': 17, 'equity': 109, 'commodity': 14}
- `16:50:33` unwind: cohort=59.0 regime=MILD_RISK_ON roro=15.7
- `16:50:33` ✅ CRYPTO REMOVED — 4 classes only: {'fx': 16, 'fixed_income': 17, 'equity': 109, 'commodity': 14}.
- `16:50:33` ✅ universe intact — 156 assets across 4 classes.
- `16:50:33` ✅ unwind overlay still regime-joined (MILD_RISK_ON).
- `16:50:33` ✅ version 1.4.0 live
