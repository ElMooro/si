## 0. Wait for deploys

**Status:** failure  
**Duration:** 1.9s  
**Finished:** 2026-07-09T21:21:42+00:00  

## Error

```
SystemExit: 1
```

## Data

| band | barometer | code_ages_min | funding_family_fallback | funding_green | funding_rows | invoke | n_canaries | n_fails | n_votes | n_warns | note | page_everything_tab | page_gauge | sentinel_informational | sentinel_rows | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | {'justhodl-canary-warroom': 0.0} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | {'errorMessage': "'<' not supported between instances of 'float' and 'str'", 'errorType': 'TypeError', 'requestId': '816273a1-afed-44ae-89d1-ae8288a6ab3b', 'stackTrace': ['  File "/var/task/lambda_function.py", line 469, in lambda_handler\n    firing.sort(key=lambda c: (-(c.get("stress") or 0), c.get("lead_months") or 99))\n']} |  |  |  |  |  |  |  |  |  |  |
| WATCH | 32.7 |  | 7 | 7 | 9 |  | 156 |  | 178 |  | Every watched canary = one equal vote of its 0-100 stress. Sentinel alert-rules (binary flips) shown separately, not ave |  |  | 0 | 0 |  |
|  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |
|  |  |  |  |  |  |  |  | 4 |  | 1 |  |  |  |  |  | FAIL |

## Log
## 1. Warroom v4 regeneration

## 3. Live page checks (CDN lag = warn-level)

- `21:21:42` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `21:21:42` FAIL: funding member rows=9 (<15)
- `21:21:42` FAIL: family aggregates present ALONGSIDE members -- double-count risk
- `21:21:42` FAIL: sentinel rows=0 (<6)
- `21:21:42` FAIL: n_votes=178 did not grow past 178
