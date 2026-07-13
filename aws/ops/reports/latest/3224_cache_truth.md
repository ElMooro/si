# ops 3224 — ids-ledger cache, polite fetchers, honest aggregates

**Status:** success  
**Duration:** 145.3s  
**Finished:** 2026-07-13T06:12:23+00:00  

## Data

| active_before | active_now | aggregates_retired | coverage_now | evicted | n_fails | n_warns | series_cached | verdict | woken |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 1 |  |  |  |  |  |
|  |  | 23 |  |  |  |  |  |  |  |
|  |  |  | 74.1 |  |  |  |  |  |  |
| 117 | 117 |  |  |  |  |  | 2314 |  | 0 |
|  |  |  |  |  | 0 | 0 |  | PASS |  |

## Log
## 1. Evict poisoned cache entries + retire fake aggregates

## 2. Deploy (config-propagation AWAITED)

- `06:10:07`   zip: 79842 bytes
## 1. Lambda

- `06:10:07`   Lambda exists — updating
- `06:10:10` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `06:10:11`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `06:10:11` ✅   ✓ target → justhodl-wl-engines
- `06:10:11` ✅   ✓ added invoke permission
- `06:10:12`   zip: 80006 bytes
## 1. Lambda

- `06:10:12`   Lambda exists — updating
- `06:10:15` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `06:10:16`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `06:10:16` ✅   ✓ target → justhodl-thesis-engine
- `06:10:16` ✅   ✓ added invoke permission
- `06:10:18`   zip: 76047 bytes
## 1. Lambda

- `06:10:18`   Lambda exists — updating
- `06:10:24` ✅   ✓ updated justhodl-symbol-dictionary
## 2. EB rule + permissions

- `06:10:25`   rule already correct: symbol-dictionary-weekly (cron(0 5 ? * SUN *))
- `06:10:25` ✅   ✓ target → justhodl-symbol-dictionary
- `06:10:25` ✅   ✓ added invoke permission
## 3. Fleet run — wakes by name

- `06:12:23`   → Europe Liquidity :BTPBUND  measure f DORMANT resolved=5
- `06:12:23`   → Global Deposit Rates Which drains li DORMANT resolved=4
