# ops 3194 — USI residue: McClellan computed, exchange variants proxied, intraday tagged

**Status:** success  
**Duration:** 13.6s  
**Finished:** 2026-07-13T02:59:45+00:00  

## Data

| coverage_before | coverage_now | curated_total | intraday_only | n_fails | n_warns | new_entries | probed | pruned | survivors | usi_unmapped | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  | 95 |  |
|  |  |  | 2 |  |  | 2 |  |  |  |  |  |
|  |  |  |  |  |  |  | 2 | 2 | 0 |  |  |
| 76.1 | 76.1 | 22 |  |  |  |  |  |  |  |  |  |
|  |  |  |  | 0 | 1 |  |  |  |  |  | PASS |

## Log
## 1. USI token census

- `02:59:32`   ADVDECV.US             1
- `02:59:32`   ATHI.DJ                1
- `02:59:32`   ATHI.US                1
- `02:59:32`   ATLO.DJ                1
- `02:59:32`   ATLO.US                1
- `02:59:32`   BASR.US                1
- `02:59:32`   BATD.NQ                1
- `02:59:32`   BATD.US                1
- `02:59:32`   BAVD.DJ                1
- `02:59:32`   BAVD.NY                1
- `02:59:32`   BAVD.US                1
- `02:59:32`   BLKS.ASK.DJ            1
- `02:59:32`   BLKS.ASK.US            1
- `02:59:32`   BLKS.BID.DJ            1
- `02:59:32`   BLKS.BID.US            1
- `02:59:32`   BLKS.DNTK.US           1
- `02:59:32`   BLKTDS.NY              1
- `02:59:32`   BLKTDS.US              1
## 2. Re-map + probe new INTERNALS/DERIVED/intraday split

## 3. Write + redeploy + kick

- `02:59:32`   zip: 76172 bytes
## 1. Lambda

- `02:59:33`   Lambda exists — updating
- `02:59:35` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `02:59:36`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `02:59:36` ✅   ✓ target → justhodl-wl-engines
- `02:59:36` ✅   ✓ added invoke permission
- `02:59:36`   zip: 77791 bytes
## 1. Lambda

- `02:59:36`   Lambda exists — updating
- `02:59:41` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `02:59:42`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `02:59:42` ✅   ✓ target → justhodl-thesis-engine
- `02:59:42` ✅   ✓ added invoke permission
- `02:59:42`   zip: 73832 bytes
## 1. Lambda

- `02:59:42`   Lambda exists — updating
- `02:59:45` ✅   ✓ updated justhodl-symbol-dictionary
## 2. EB rule + permissions

- `02:59:45`   rule already correct: symbol-dictionary-weekly (cron(0 5 ? * SUN *))
- `02:59:45` ✅   ✓ target → justhodl-symbol-dictionary
- `02:59:45` ✅   ✓ added invoke permission
- `02:59:45` ⚠ all new USI entries probed dry — internals feed history may be shorter than MIN_POINTS
