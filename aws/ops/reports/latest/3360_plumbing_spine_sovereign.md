## 1. Deploy + verify sovereign-stress fix

**Status:** success  
**Duration:** 52.3s  
**Finished:** 2026-07-15T19:51:15+00:00  

## Log
- `19:50:23`   zip: 81698 bytes
## 1. Lambda

- `19:50:23`   Lambda exists — updating
- `19:50:26` ✅   ✓ updated justhodl-sovereign-stress
## 2. EB rule + permissions

- `19:50:26`   rule already correct: sovereign-stress-daily (cron(0 12 ? * * *))
- `19:50:26` ✅   ✓ target → justhodl-sovereign-stress
- `19:50:26` ✅   ✓ added invoke permission
- `19:50:53`   return: {"statusCode": 200, "body": "{\"ok\": true, \"regime\": \"NORMAL\", \"europe_score\": 30.7, \"errors\": 2}"}
- `19:50:56`   europe score_0_100=30.7 regime=NORMAL worst=portugal errors=2
- `19:50:56`   most-stressed sovereign: france
- `19:50:56` ✅ SOVEREIGN-STRESS FIXED — errors 2 (was 11), real score 30.7.
## 2. Deploy JSI calibrator (9-component spine)

- `19:50:56`   zip: 80139 bytes
## 1. Lambda

- `19:50:56`   Lambda exists — updating
- `19:50:59` ✅   ✓ updated justhodl-jsi-calibrator
## 2. EB rule + permissions

- `19:50:59`   rule already correct: jsi-calibrator-weekly (cron(30 9 ? * SUN *))
- `19:50:59` ✅   ✓ target → justhodl-jsi-calibrator
- `19:50:59` ✅   ✓ added invoke permission
- `19:50:59`   calibrator invoked (async); will verify spine picks up after JSI run
## 3. Deploy JSI v1.4.0 (reserves + Fed-BS spine)

- `19:50:59`   zip: 82371 bytes
## 1. Lambda

- `19:50:59`   Lambda exists — updating
- `19:51:04` ✅   ✓ updated justhodl-stress-index
## 2. EB rule + permissions

- `19:51:05`   rule already correct: jsi-6h (rate(6 hours))
- `19:51:05` ✅   ✓ target → justhodl-stress-index
- `19:51:05` ✅   ✓ added invoke permission
- `19:51:11`   JSI v1.4.0 jsi=35.13 spine=39.22 regime=NORMAL pctile=30.4
- `19:51:11`   spine components (9): ['VIXCLS', 'NFCI', 'KCFSI', 'STLFSI4', 'BAMLH0A0HYM2', 'T10Y2Y', 'BAMLC0A0CM', 'WRESBAL', 'WALCL']
- `19:51:11`     WRESBAL: Bank Reserves (draining) chg=-30677.0 stress=57.46 z=0.27 mode=chg
- `19:51:11`     WALCL: Fed Balance Sheet (QT) chg=29913.0 stress=52.86 z=0.1 mode=chg
- `19:51:11` ✅ LIQUIDITY PLUMBING in spine — Bank Reserves + Fed Balance Sheet now historical components (brain directive).
- `19:51:11` ✅ index coherent — 9467 pts, 30.4th pctile since 1990.
- `19:51:15`   calibrator spine: n=2502 components=['VIXCLS', 'NFCI', 'KCFSI', 'STLFSI4', 'BAMLH0A0HYM2', 'T10Y2Y', 'BAMLC0A0CM', 'WRESBAL', 'WALCL']
- `19:51:15` ✅ calibrator now fits all 9 — reserves IC=-0.0083, Fed-BS IC=-0.0486.
