# ops 3173 — regime truncation fix + compass traceback

**Status:** failure  
**Duration:** 126.3s  
**Finished:** 2026-07-12T22:58:54+00:00  

## Error

```
SystemExit: 1
```

## Data

| n_fails | n_warns | regime_now | verdict | weeks_easing | weeks_neutral | weeks_tightening |
|---|---|---|---|---|---|---|
|  |  | NEUTRAL |  | 0 | 1746 | 0 |
| 2 | 0 |  | FAIL |  |  |  |

## Log
## 1. Compass — the verbatim failure

- `22:56:49` errorType: NameError
- `22:56:49` errorMessage: name 'fetch_json' is not defined
- `22:56:49`   File "/var/task/_sentry_lite.py", line 68, in wrapped
    return handler(event, context)
- `22:56:49`   File "/var/task/lambda_function.py", line 847, in handler
    for k, v in ((fetch_json("data/notes-index.json") or {})
## 2. Thesis engine — regime series fetched FIRST

- `22:56:50`   zip: 66447 bytes
## 1. Lambda

- `22:56:50`   Lambda exists — updating
- `22:56:53` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `22:56:53`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `22:56:53` ✅   ✓ target → justhodl-thesis-engine
- `22:56:53` ✅   ✓ added invoke permission
- `22:58:54`   debug: {"ff_obs": 0, "bs_obs": 0, "ff_non_null": 0, "ff_first": null, "ff_last": null}
- `22:58:54` ✗ compass: NameError: name 'fetch_json' is not defined
- `22:58:54` ✗ regime STILL degenerate: {'EASING': 0, 'NEUTRAL': 1746, 'TIGHTENING': 0} debug={'ff_obs': 0, 'bs_obs': 0, 'ff_non_null': 0, 'ff_first': None, 'ff_last': None}
