# Verify Phase 2 dual-write — ka_* aliases in S3 outputs

**Status:** success  
**Duration:** 69.1s  
**Finished:** 2026-04-26T12:55:14+00:00  

## Log
## A. Force-invoke producer Lambdas to bake fresh outputs

- `12:54:07`   🟢 justhodl-intelligence            2.1s  err=none
- `12:54:18` ⚠   ✗ justhodl-daily-report-v3: An error occurred (TooManyRequestsException) when calling the Invoke operation (reached max retries: 4): Rate Exceeded.
- `12:54:19`   🟢 justhodl-pnl-tracker             1.2s  err=none
- `12:55:07`   🟢 justhodl-crypto-intel           47.3s  err=none
## B. Verify ka_* aliases in S3 outputs

## 📦 justhodl-intelligence → s3://justhodl-dashboard-live/intelligence-report.json

- `12:55:12`   size: 4747B  age: 0.02h
- `12:55:12`   khalid_* keys found: ['khalid_index']
- `12:55:12`   ka_* keys found:     ['ka_index']
- `12:55:12`   no khalid_ keys at top scan
## 📦 justhodl-daily-report-v3 → s3://justhodl-dashboard-live/data/report.json

- `12:55:13`   size: 1751210B  age: 0.09h
- `12:55:13`   khalid_* keys found: ['khalid_index']
- `12:55:13`   ka_* keys found:     []
- `12:55:13` ⚠   ⚠ khalid_index present but ka_index missing
## 📦 justhodl-pnl-tracker → s3://justhodl-dashboard-live/portfolio/pnl-daily.json

- `12:55:13`   size: 1502B  age: 0.01h
- `12:55:13`   khalid_* keys found: ['khalid_strategy']
- `12:55:13`   ka_* keys found:     ['ka_strategy']
- `12:55:13`   no khalid_ keys at top scan
## 📦 justhodl-reports-builder → s3://justhodl-dashboard-live/scorecard.json

- `12:55:13` ⚠   S3 key not found
## 📦 justhodl-bloomberg-v8 → s3://justhodl-dashboard-live/data/report.json

- `12:55:13`   size: 1751210B  age: 0.09h
- `12:55:13`   khalid_* keys found: ['khalid_index']
- `12:55:13`   ka_* keys found:     []
- `12:55:13` ⚠   ⚠ khalid_index present but ka_index missing
## 📦 justhodl-crypto-intel → s3://justhodl-dashboard-live/crypto-intel.json

- `12:55:13`   size: 57491B  age: 0.00h
- `12:55:13`   khalid_* keys found: ['khalid_index']
- `12:55:13`   ka_* keys found:     ['ka_index']
- `12:55:13`   ▸ DUAL-WRITE-OK: 1 ka_* aliases for 1 khalid_* keys
## 📦 justhodl-calibrator → s3://justhodl-dashboard-live/calibration/latest.json

- `12:55:14`   size: 5314B  age: 3.91h
- `12:55:14`   khalid_* keys found: ['khalid_component_weights', 'khalid_index']
- `12:55:14`   ka_* keys found:     []
- `12:55:14` ⚠   ⚠ khalid_component_weights present but ka_component_weights missing
- `12:55:14` ⚠   ⚠ khalid_index present but ka_index missing
## FINAL

- `12:55:14`   DUAL-WRITE-OK: 1
- `12:55:14`   NO-DATA: 1
- `12:55:14`   NO-KHALID-KEYS: 2
- `12:55:14`   PARTIAL-DUAL: 3
- `12:55:14` 
  1/7 producers fully dual-writing
- `12:55:14` Done
