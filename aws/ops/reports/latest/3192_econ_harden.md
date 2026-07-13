# ops 3192 — purge cross-country poison, learn templates, retire licensed

**Status:** success  
**Duration:** 24.7s  
**Finished:** 2026-07-13T02:52:36+00:00  

## Data

| audited | coverage_before | coverage_now | curated_total | licensed_econ | n_fails | n_warns | purged | template_hits | template_probed | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 11 |  |  |  |  |  |  | 9 |  |  |  |
|  |  |  |  |  |  |  |  | 19 | 28 |  |
|  |  |  |  | 16 |  |  |  |  |  |  |
|  | 75.8 | 75.9 | 21 |  |  |  |  |  |  |  |
|  |  |  |  |  | 0 | 0 |  |  |  | PASS |

## Log
## 1. Audit + purge 3191 search entries

- `02:52:12`   ✗ PURGED ECONOMICS:AEINTR → BIS/WS_DEBT_SEC2_PUB/Q.3P.AE.1.1.C.A.A.EU1.A.A.A.A.A.C (country/concept mismatch)
- `02:52:12`   ✗ PURGED ECONOMICS:BRCLI → OECD/DSD_KEI@DF_KEI/AUS.M.LI.IX._T.AA._Z (country/concept mismatch)
- `02:52:12`   ✗ PURGED ECONOMICS:BHINTR → BIS/WS_DEBT_SEC2_PUB/Q.3P.BH.1.1.C.A.A.EU1.A.A.A.A.A.G (country/concept mismatch)
- `02:52:12`   ✗ PURGED ECONOMICS:CNCLI → OECD/DSD_KEI@DF_KEI/AUS.M.LI.IX._T.AA._Z (country/concept mismatch)
- `02:52:12`   ✗ PURGED ECONOMICS:MAINTR → BIS/WS_DEBT_SEC2_PUB/Q.3P.MA.1.1.C.A.A.EU1.A.A.A.A.A.I (country/concept mismatch)
- `02:52:12`   ✗ PURGED ECONOMICS:QAINTR → BIS/WS_DEBT_SEC2_PUB/Q.3P.QA.1.1.C.A.A.EU1.A.A.A.A.A.C (country/concept mismatch)
- `02:52:12`   ✗ PURGED ECONOMICS:MUINTR → BIS/WS_DEBT_SEC2_PUB/Q.3P.MU.1.1.C.A.A.EU1.A.A.A.A.A.I (country/concept mismatch)
- `02:52:12`   ✗ PURGED ECONOMICS:PAINTR → BIS/WS_DEBT_SEC2_PUB/Q.3P.PA.1.1.C.A.A.EU1.A.A.A.A.A.I (country/concept mismatch)
- `02:52:12`   ✗ PURGED ECONOMICS:TNINTR → BIS/WS_DEBT_SEC2_PUB/Q.3P.TN.1.1.C.A.A.EU1.A.A.A.A.A.I (country/concept mismatch)
## 2. Learned-template probe (CLI/BLR/INTR/BP, all countries)

- `02:52:21`   CLI    probed   2  hit   0
- `02:52:21`   BLR    probed   4  hit   0
- `02:52:21`   INTR   probed  21  hit  19
- `02:52:21`   BP     probed   1  hit   0
- `02:52:21`     ✓ ECONOMICS:CNINTR → IMF/IFS/M.CN.FILR_PA  (426 pts)
- `02:52:21`     ✓ ECONOMICS:MYINTR → IMF/IFS/M.MY.FILR_PA  (425 pts)
- `02:52:21`     ✓ ECONOMICS:MUINTR → IMF/IFS/M.MU.FILR_PA  (422 pts)
- `02:52:21`     ✓ ECONOMICS:EGINTR → IMF/IFS/M.EG.FILR_PA  (421 pts)
- `02:52:21`     ✓ ECONOMICS:HKINTR → IMF/IFS/M.HK.FILR_PA  (416 pts)
## 3. Licensed retirement (MPMI/LEI class)

- `02:52:21`   retired: ECONOMICS:CHMPMI (S&P Global / Conference Board — no free primary)
- `02:52:21`   retired: ECONOMICS:CLLEI (S&P Global / Conference Board — no free primary)
- `02:52:21`   retired: ECONOMICS:CNLEI (S&P Global / Conference Board — no free primary)
- `02:52:21`   retired: ECONOMICS:CNMPMI (S&P Global / Conference Board — no free primary)
- `02:52:21`   retired: ECONOMICS:CNSPMI (S&P Global / Conference Board — no free primary)
- `02:52:21`   retired: ECONOMICS:DEMPMI (S&P Global / Conference Board — no free primary)
## 4. Write + hard assertion

- `02:52:21`   zip: 74913 bytes
## 1. Lambda

- `02:52:21`   Lambda exists — updating
- `02:52:26` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `02:52:27`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `02:52:27` ✅   ✓ target → justhodl-wl-engines
- `02:52:27` ✅   ✓ added invoke permission
- `02:52:27`   zip: 76532 bytes
## 1. Lambda

- `02:52:27`   Lambda exists — updating
- `02:52:30` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `02:52:30`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `02:52:30` ✅   ✓ target → justhodl-thesis-engine
- `02:52:30` ✅   ✓ added invoke permission
- `02:52:30`   zip: 72573 bytes
## 1. Lambda

- `02:52:31`   Lambda exists — updating
- `02:52:36` ✅   ✓ updated justhodl-symbol-dictionary
## 2. EB rule + permissions

- `02:52:36`   rule already correct: symbol-dictionary-weekly (cron(0 5 ? * SUN *))
- `02:52:36` ✅   ✓ target → justhodl-symbol-dictionary
- `02:52:36` ✅   ✓ added invoke permission
