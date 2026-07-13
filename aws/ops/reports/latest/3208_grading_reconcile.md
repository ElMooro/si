# ops 3208 — panels enter the trust ledger; nothing is silently dropped

**Status:** failure  
**Duration:** 162.1s  
**Finished:** 2026-07-13T04:58:17+00:00  

## Error

```
SystemExit: 1
```

## Data

| active | dormant | firing | index_engines | lists | n_fails | n_warns | series_cached | verdict | wl_signals_in_ledger |
|---|---|---|---|---|---|---|---|---|---|
| 115 | 92 | 24 | 207 | 207 |  |  | 2281 |  |  |
|  |  |  |  |  |  |  |  |  | 0 |
|  |  |  |  |  | 1 | 1 |  | FAIL |  |

## Log
## 1. Deploy patched runner + full run

- `04:55:36`   zip: 78819 bytes
## 1. Lambda

- `04:55:36`   Lambda exists — updating
- `04:55:45` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `04:55:45`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `04:55:45` ✅   ✓ target → justhodl-wl-engines
- `04:55:45` ✅   ✓ added invoke permission
## 2. Reconcile 207-vs-162

- `04:57:08`   DORMANT  47 × needs >=6 members on a free source — map more of its indicators to act
- `04:57:08`   DORMANT  39 × mapped members lack fetchable history
- `04:57:08`   DORMANT   3 × only 0 weeks of joint activation history
- `04:57:08`   DORMANT   1 × only 20 weeks of joint activation history
- `04:57:08`   DORMANT   1 × only 28 weeks of joint activation history
- `04:57:08` ✅ every list accounted for — 207/207, zero silent drops
## 3. wl_ signals in DynamoDB

## 4. Outcome-checker ingest

- `04:58:17` ⚠ checker invoke: An error occurred (TooManyRequestsException) when calling the Invoke operation (
- `04:58:17` ✗ firing panels exist but zero wl_ signals landed
