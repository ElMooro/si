# Ops 1530 — exponential layer deploy

**Status:** success  
**Duration:** 27.3s  
**Finished:** 2026-06-09T23:27:00+00:00  

## Log
## 0. IAM events grant

- `23:26:33` ✅ smartwake-events policy attached
## A. global-tide

- `23:26:33`   zip: 4820 bytes
## 1. Lambda

- `23:26:33`   Lambda missing — creating
- `23:26:38` ✅   ✓ created justhodl-global-tide
- `23:26:38` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `23:26:39` ✅   ✓ created rule justhodl-global-tide-daily
- `23:26:39` ✅   ✓ target → justhodl-global-tide
- `23:26:39` ✅   ✓ added invoke permission
## 3. Smoke test

- `23:26:39`   invoking justhodl-global-tide…
- `23:26:41` ✅   ✓ smoke test passed
- `23:26:41`     regime                   EBBING
- `23:26:41`     risk                     40.0
- `23:26:41`     n_flashing               1
## B. apex-fusion

- `23:26:41`   zip: 4148 bytes
## 1. Lambda

- `23:26:41`   Lambda missing — creating
- `23:26:46` ✅   ✓ created justhodl-apex-fusion
- `23:26:46` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `23:26:47` ✅   ✓ created rule justhodl-apex-fusion-3h
- `23:26:47` ✅   ✓ target → justhodl-apex-fusion
- `23:26:47` ✅   ✓ added invoke permission
## 3. Smoke test

- `23:26:47`   invoking justhodl-apex-fusion…
- `23:26:49` ✅   ✓ smoke test passed
- `23:26:49`     n                        219
- `23:26:49`     logged                   3
## C. smart-wake

- `23:26:49`   zip: 2867 bytes
## 1. Lambda

- `23:26:49`   Lambda missing — creating
- `23:26:54` ✅   ✓ created justhodl-smart-wake
- `23:26:54` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `23:26:54` ✅   ✓ created rule justhodl-smart-wake-hourly
- `23:26:54` ✅   ✓ target → justhodl-smart-wake
- `23:26:54` ✅   ✓ added invoke permission
## 3. Smoke test

- `23:26:54`   invoking justhodl-smart-wake…
- `23:26:55` ✅   ✓ smoke test passed
- `23:26:55`     mode                     SLEEPING
- `23:26:55`     actions                  0
## D. cascade-validator daily rule

- `23:26:55` ✅   ✓ created rule justhodl-cascade-validator-daily
- `23:26:56` ✅   ✓ target → justhodl-cascade-validator
- `23:26:56` ✅   ✓ added invoke permission
## E. verify briefs

- `23:27:00` [{"ticker": "QNT", "apex_score": 100, "tier": "SIMMER", "n_sources": 1, "sources": ["insider"]}, {"ticker": "NTAP", "apex_score": 89.8, "tier": "IGNITION", "n_sources": 2, "sources": ["momentum", "pump"]}, {"ticker": "BB", "apex_score": 87.7, "tier": "SIMMER", "n_sources": 1, "sources": ["momentum"]}, {"ticker": "DXCM", "apex_score": 85.9, "tier": "IGNITION", "n_sources": 2, "sources": ["momentum", "pump"]}, {"ticker": "NBIS", "apex_score": 80.5, "tier": "IGNITION", "n_sources": 2, "sources": ["momentum", "pump"]}, {"ticker": "FTNT", "apex_score": 80.4, "tier": "SIMMER", "n_sources": 1, "sources": ["momentum"]}]
