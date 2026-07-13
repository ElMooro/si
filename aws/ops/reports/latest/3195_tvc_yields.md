# ops 3195 — TVC residue: non-OECD yields via IMF IFS

**Status:** success  
**Duration:** 17.4s  
**Finished:** 2026-07-13T03:05:54+00:00  

## Data

| coverage_before | coverage_now | curated_total | n_fails | n_warns | probed | pruned | survivors | tvc_unmapped | verdict | yield_tiles |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | 91 |  | 23 |
|  |  |  |  |  | 23 | 15 | 8 |  |  |  |
| 76.1 | 76.5 | 30 |  |  |  |  |  |  |  |  |
|  |  |  | 0 | 0 |  |  |  |  | PASS |  |

## Log
## 1. TVC census

- `03:05:37`   AXY              1
- `03:05:37`   BR10Y            1
- `03:05:37`   BTPBUND          1
- `03:05:37`   CN01Y            1
- `03:05:37`   CN02Y            1
- `03:05:37`   CN05Y            1
- `03:05:37`   CN10             1
- `03:05:37`   CN10Y            1
- `03:05:37`   CN30Y            1
- `03:05:37`   DE01             1
- `03:05:37`   DE10             1
- `03:05:37`   DE20             1
- `03:05:37`   DE30             1
- `03:05:37`   DEU40            1
- `03:05:37`   ES10             1
- `03:05:37`   EU01             1
## 2. Re-map + probe the IFS yield entries

- `03:05:41`     ✓ TVC:ZA10Y → IMF/IFS/M.ZA.FIGB_PA  (423 pts)
- `03:05:41`     ✓ TVC:SG02Y → IMF/IFS/M.SG.FIGB_PA  (277 pts)
- `03:05:41`     ✓ TVC:SG10Y → IMF/IFS/M.SG.FIGB_PA  (277 pts)
- `03:05:41`     ✓ TVC:RU10Y → IMF/IFS/M.RU.FIGB_PA  (157 pts)
- `03:05:41`     ✓ TVC:IN02Y → IMF/IFS/M.IN.FIGB_PA  (149 pts)
- `03:05:41`     ✓ TVC:IN10Y → IMF/IFS/M.IN.FIGB_PA  (149 pts)
## 3. Write + redeploy + kick

- `03:05:41`   zip: 76775 bytes
## 1. Lambda

- `03:05:41`   Lambda exists — updating
- `03:05:44` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `03:05:45`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `03:05:45` ✅   ✓ target → justhodl-wl-engines
- `03:05:45` ✅   ✓ added invoke permission
- `03:05:45`   zip: 78394 bytes
## 1. Lambda

- `03:05:45`   Lambda exists — updating
- `03:05:50` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `03:05:50`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `03:05:50` ✅   ✓ target → justhodl-thesis-engine
- `03:05:50` ✅   ✓ added invoke permission
- `03:05:51`   zip: 74435 bytes
## 1. Lambda

- `03:05:51`   Lambda exists — updating
- `03:05:54` ✅   ✓ updated justhodl-symbol-dictionary
## 2. EB rule + permissions

- `03:05:54`   rule already correct: symbol-dictionary-weekly (cron(0 5 ? * SUN *))
- `03:05:54` ✅   ✓ target → justhodl-symbol-dictionary
- `03:05:54` ✅   ✓ added invoke permission
