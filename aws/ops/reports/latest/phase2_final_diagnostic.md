# Phase 2 final diagnostic + force-invoke pass

**Status:** success  
**Duration:** 186.5s  
**Finished:** 2026-04-26T13:03:24+00:00  

## Log
## A. Deployed-code audit — does each Lambda have the patch live?

- `13:00:18`   ✅ justhodl-intelligence (17397B zip) — helper module + add_ka_aliases() call present
- `13:00:18`   ✅ justhodl-daily-report-v3 (33387B zip) — helper module + add_ka_aliases() call present
- `13:00:18`   ✅ justhodl-pnl-tracker (5737B zip) — helper module + add_ka_aliases() call present
- `13:00:18`   ✅ justhodl-investor-agents (7310B zip) — helper module + add_ka_aliases() call present
- `13:00:18`   ✅ justhodl-morning-intelligence (11548B zip) — helper module + add_ka_aliases() call present
- `13:00:19`   ✅ justhodl-reports-builder (7188B zip) — helper module + add_ka_aliases() call present
- `13:00:19`   ✅ justhodl-signal-logger (8137B zip) — helper module + add_ka_aliases() call present
- `13:00:19`   ✅ justhodl-bloomberg-v8 (9930B zip) — helper module + add_ka_aliases() call present
- `13:00:19`   ✅ justhodl-crypto-intel (15202B zip) — helper module + add_ka_aliases() call present
- `13:00:20`   ✅ justhodl-calibrator (7004B zip) — helper module + add_ka_aliases() call present
## B. Locate reports-builder S3 output

- `13:00:20`   SCORECARD_KEY defs found: ['reports/scorecard.json']
- `13:00:20`   All S3 keys in source: ['SCORECARD_KEY']
## C. Force-invoke all 10 producers

- `13:00:22`   ✅ justhodl-intelligence            1.8s  err=none
- `13:01:30` ⚠   ✗ justhodl-daily-report-v3: An error occurred (TooManyRequestsException) when calling the Invoke operation (reached max retries: 4): Rate Exceeded.
- `13:01:33`   ✅ justhodl-pnl-tracker             1.2s  err=none
- `13:01:36`   ✅ justhodl-investor-agents         0.8s  err=none
- `13:01:53`   ✅ justhodl-morning-intelligence   15.1s  err=none
- `13:02:06`   ✅ justhodl-reports-builder        10.8s  err=none
- `13:02:19`   ✅ justhodl-signal-logger          10.6s  err=none
- `13:02:22`   ✅ justhodl-bloomberg-v8            1.4s  err=none
- `13:03:10`   ✅ justhodl-crypto-intel           45.4s  err=none
- `13:03:13`   ❌ justhodl-calibrator              1.3s  err=Unhandled
- `13:03:13`       payload preview: {"errorMessage": "An error occurred (ValidationException) when calling the PutParameter operation: Standard tier parameters support a maximum parameter value of 4096 characters. To create a larger parameter value, upgrade the parameter to use the advanced-parameter tier. For more information, see ht
## D. Verify ka_* aliases in S3 outputs (FIXED verdict logic)

## 📦 justhodl-intelligence → s3://justhodl-dashboard-live/intelligence-report.json

- `13:03:23`   size: 4747B  age: 181.4s
- `13:03:23`   khalid_* keys (1): ['khalid_index']
- `13:03:23`   ka_* keys     (1): ['ka_index']
- `13:03:23`   ▸ ✅ DUAL-WRITE-OK — all 1 khalid_* keys have ka_* aliases
## 📦 justhodl-daily-report-v3 → s3://justhodl-dashboard-live/data/report.json

- `13:03:23`   size: 1751416B  age: 64.7s
- `13:03:23`   khalid_* keys (1): ['khalid_index']
- `13:03:23`   ka_* keys     (1): ['ka_index']
- `13:03:23`   ▸ ✅ DUAL-WRITE-OK — all 1 khalid_* keys have ka_* aliases
## 📦 justhodl-pnl-tracker → s3://justhodl-dashboard-live/portfolio/pnl-daily.json

- `13:03:23`   size: 1502B  age: 109.8s
- `13:03:23`   khalid_* keys (1): ['khalid_strategy']
- `13:03:23`   ka_* keys     (1): ['ka_strategy']
- `13:03:23`   ▸ ✅ DUAL-WRITE-OK — all 1 khalid_* keys have ka_* aliases
## 📦 justhodl-bloomberg-v8 → s3://justhodl-dashboard-live/data/report.json

- `13:03:23`   size: 1751416B  age: 64.9s
- `13:03:23`   khalid_* keys (1): ['khalid_index']
- `13:03:23`   ka_* keys     (1): ['ka_index']
- `13:03:23`   ▸ ✅ DUAL-WRITE-OK — all 1 khalid_* keys have ka_* aliases
## 📦 justhodl-crypto-intel → s3://justhodl-dashboard-live/crypto-intel.json

- `13:03:24`   size: 57259B  age: 14.1s
- `13:03:24`   khalid_* keys (1): ['khalid_index']
- `13:03:24`   ka_* keys     (1): ['ka_index']
- `13:03:24`   ▸ ✅ DUAL-WRITE-OK — all 1 khalid_* keys have ka_* aliases
## 📦 justhodl-calibrator → s3://justhodl-dashboard-live/calibration/latest.json

- `13:03:24`   size: 5314B  age: 14556.1s
- `13:03:24`   khalid_* keys (2): ['khalid_component_weights', 'khalid_index']
- `13:03:24`   ka_* keys     (0): []
- `13:03:24` ⚠   ✗ OLD-ONLY: 0 ka_* aliases for 2 khalid_* keys
## FINAL SUMMARY

- `13:03:24` 
Deployment status:
- `13:03:24`   DEPLOYED   justhodl-intelligence
- `13:03:24`   DEPLOYED   justhodl-daily-report-v3
- `13:03:24`   DEPLOYED   justhodl-pnl-tracker
- `13:03:24`   DEPLOYED   justhodl-investor-agents
- `13:03:24`   DEPLOYED   justhodl-morning-intelligence
- `13:03:24`   DEPLOYED   justhodl-reports-builder
- `13:03:24`   DEPLOYED   justhodl-signal-logger
- `13:03:24`   DEPLOYED   justhodl-bloomberg-v8
- `13:03:24`   DEPLOYED   justhodl-crypto-intel
- `13:03:24`   DEPLOYED   justhodl-calibrator
- `13:03:24` 
Invoke status:
- `13:03:24`   OK           justhodl-intelligence
- `13:03:24`   INVOKE-FAIL  justhodl-daily-report-v3
- `13:03:24`   OK           justhodl-pnl-tracker
- `13:03:24`   OK           justhodl-investor-agents
- `13:03:24`   OK           justhodl-morning-intelligence
- `13:03:24`   OK           justhodl-reports-builder
- `13:03:24`   OK           justhodl-signal-logger
- `13:03:24`   OK           justhodl-bloomberg-v8
- `13:03:24`   OK           justhodl-crypto-intel
- `13:03:24`   ERR          justhodl-calibrator
- `13:03:24` 
Dual-write verdict:
- `13:03:24`   DUAL-WRITE-OK  justhodl-intelligence
- `13:03:24`   DUAL-WRITE-OK  justhodl-daily-report-v3
- `13:03:24`   DUAL-WRITE-OK  justhodl-pnl-tracker
- `13:03:24`   DUAL-WRITE-OK  justhodl-bloomberg-v8
- `13:03:24`   DUAL-WRITE-OK  justhodl-crypto-intel
- `13:03:24`   OLD-ONLY       justhodl-calibrator
- `13:03:24` 
  5/6 producers fully aliased (or N/A)
- `13:03:24` Done
