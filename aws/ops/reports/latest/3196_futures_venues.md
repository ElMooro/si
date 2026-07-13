# ops 3196 — EU futures proxied, venues deduped, CME roots closed

**Status:** success  
**Duration:** 15.2s  
**Finished:** 2026-07-13T03:06:10+00:00  

## Data

| coverage_before | coverage_now | curated_total | n_fails | n_warns | new_proxy_entries | probed | pruned | survivors | venue_duplicates_resolved | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 0 |  |  |  |  |  |
|  |  |  |  |  |  | 0 | 0 | 0 |  |  |
|  |  |  |  |  |  |  |  |  | 0 |  |
| 76.5 | 76.8 | 30 |  |  |  |  |  |  |  |  |
|  |  |  | 0 | 0 |  |  |  |  |  | PASS |

## Log
## 1. Root census (EUREX/ICEEUR/CME residue)

- `03:05:54`   UB           2
- `03:05:54`   GE           2
- `03:05:54`   SR           2
- `03:05:54`   FMAG         2
- `03:05:54`   FMXU         2
- `03:05:54`   FMXX         2
- `03:05:54`   FVS          2
- `03:05:54`   I            2
- `03:05:54`   ME           2
- `03:05:54`   SF           2
- `03:05:54`   UEW          2
- `03:05:54`   AW           1
- `03:05:54`   TN           1
- `03:05:54`   YIT          1
- `03:05:54`   ZQ           1
- `03:05:54`   6M           1
- `03:05:54`   BSB          1
- `03:05:54`   BTC          1
## 2. Re-map (proxies + new roots) + probe

## 3. Venue dedupe → primaries

## 4. Write + redeploy + kick

- `03:05:55`   zip: 76775 bytes
## 1. Lambda

- `03:05:55`   Lambda exists — updating
- `03:05:58` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `03:05:58`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `03:05:58` ✅   ✓ target → justhodl-wl-engines
- `03:05:58` ✅   ✓ added invoke permission
- `03:05:58`   zip: 78394 bytes
## 1. Lambda

- `03:05:59`   Lambda exists — updating
- `03:06:04` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `03:06:04`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `03:06:04` ✅   ✓ target → justhodl-thesis-engine
- `03:06:04` ✅   ✓ added invoke permission
- `03:06:04`   zip: 74435 bytes
## 1. Lambda

- `03:06:04`   Lambda exists — updating
- `03:06:09` ✅   ✓ updated justhodl-symbol-dictionary
## 2. EB rule + permissions

- `03:06:09`   rule already correct: symbol-dictionary-weekly (cron(0 5 ? * SUN *))
- `03:06:09` ✅   ✓ target → justhodl-symbol-dictionary
- `03:06:10` ✅   ✓ added invoke permission
