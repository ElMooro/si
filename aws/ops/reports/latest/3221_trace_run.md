# ops 3221 — the five curations traced end-to-end

**Status:** failure  
**Duration:** 113.5s  
**Finished:** 2026-07-13T06:00:37+00:00  

## Error

```
SystemExit: 1
```

## Data

| active_now | n_fails | n_warns | verdict |
|---|---|---|---|
| 117 |  |  |  |
|  | 1 | 0 | FAIL |

## Log
## 1. Deploy instrumented runner (cold) + traced run

- `05:58:43`   zip: 79392 bytes
## 1. Lambda

- `05:58:44`   Lambda exists — updating
- `05:58:53` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `05:58:54`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `05:58:54` ✅   ✓ target → justhodl-wl-engines
- `05:58:54` ✅   ✓ added invoke permission
## 2. The trace, verbatim

## 3. The two engines now

- `06:00:37`   Europe Liquidity :BTPBUND  measure finan state=DORMANT resolved=5 reason=mapped members lack fetchable history (only 5 z-scorable of 
- `06:00:37`   Global Deposit Rates Which drains liquid state=DORMANT resolved=4 reason=mapped members lack fetchable history (only 4 z-scorable of 
- `06:00:37` ✗ no trace lines found — tracer did not run
