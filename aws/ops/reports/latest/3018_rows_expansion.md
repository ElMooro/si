## 0. Wait for deploys

**Status:** failure  
**Duration:** 988.3s  
**Finished:** 2026-07-09T21:18:01+00:00  

## Error

```
SystemExit: 1
```

## Data

| band | barometer | code_ages_min | funding_family_fallback | funding_green | funding_rows | invoke | n_canaries | n_fails | n_votes | n_warns | note | page_everything_tab | page_gauge | sentinel_informational | sentinel_rows | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | {'justhodl-canary-warroom': 0.0, 'justhodl-signal-board': 'STALE', 'justhodl-morning-intelligence': 'STALE'} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | {'errorMessage': "'<' not supported between instances of 'float' and 'str'", 'errorType': 'TypeError', 'requestId': '298615d4-6371-4b0d-9515-c93ad22e3829', 'stackTrace': ['  File "/var/task/lambda_function.py", line 469, in lambda_handler\n    firing.sort(key=lambda c: (-(c.get("stress") or 0), c.get("lead_months") or 99))\n']} |  |  |  |  |  |  |  |  |  |  |
| WATCH | 32.7 |  | 7 | 7 | 9 |  | 156 |  | 178 |  | Every watched canary = one equal vote of its 0-100 stress. Sentinel alert-rules (binary flips) shown separately, not ave |  |  | 0 | 0 |  |
|  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |
|  |  |  |  |  |  |  |  | 4 |  | 1 |  |  |  |  |  | FAIL |

## Log
## 1. Warroom v4 regeneration

## 3. Live page checks (CDN lag = warn-level)

- `21:18:01` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `21:18:01` FAIL: funding member rows=9 (<15)
- `21:18:01` FAIL: family aggregates present ALONGSIDE members -- double-count risk
- `21:18:01` FAIL: sentinel rows=0 (<6)
- `21:18:01` FAIL: n_votes=178 did not grow past 178
