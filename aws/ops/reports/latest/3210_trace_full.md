# ops 3210 — full traceback + shape guards + ledger settled

**Status:** failure  
**Duration:** 15.7s  
**Finished:** 2026-07-13T05:02:56+00:00  

## Error

```
SystemExit: 1
```

## Data

| n_fails | n_warns | verdict | wl_signals_in_ledger |
|---|---|---|---|
|  |  |  | 0 |
| 1 | 0 | FAIL |  |

## Log
## 1. The complete [ERROR] frames

- `05:02:41`   [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
- `05:02:41`   Traceback (most recent call last):
- `05:02:41`     File "/var/task/lambda_function.py", line 278, in lambda_handler
- `05:02:41`       for k, w in ex.map(pull, todo):
- `05:02:41`     File "/var/lang/lib/python3.12/concurrent/futures/_base.py", line 619, in result_iterator
- `05:02:41`       yield _result_or_cancel(fs.pop())
- `05:02:41`     File "/var/lang/lib/python3.12/concurrent/futures/_base.py", line 317, in _result_or_cancel
- `05:02:41`       return fut.result(timeout)
- `05:02:41`     File "/var/lang/lib/python3.12/concurrent/futures/_base.py", line 456, in result
- `05:02:41`       return self.__get_result()
- `05:02:41`     File "/var/lang/lib/python3.12/concurrent/futures/_base.py", line 401, in __get_result
- `05:02:41`       raise self._exception
- `05:02:41`     File "/var/lang/lib/python3.12/concurrent/futures/thread.py", line 59, in run
- `05:02:41`       result = self.fn(*self.args, **self.kwargs)
## 2. Deploy shape-guarded shared bundle

- `05:02:41`   zip: 78903 bytes
## 1. Lambda

- `05:02:41`   Lambda exists — updating
- `05:02:44` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `05:02:44`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `05:02:44` ✅   ✓ target → justhodl-wl-engines
- `05:02:44` ✅   ✓ added invoke permission
- `05:02:45`   zip: 79429 bytes
## 1. Lambda

- `05:02:45`   Lambda exists — updating
- `05:02:50` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `05:02:50`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `05:02:50` ✅   ✓ target → justhodl-thesis-engine
- `05:02:50` ✅   ✓ added invoke permission
- `05:02:50`   zip: 75470 bytes
## 1. Lambda

- `05:02:51`   Lambda exists — updating
- `05:02:55` ✅   ✓ updated justhodl-symbol-dictionary
## 2. EB rule + permissions

- `05:02:56`   rule already correct: symbol-dictionary-weekly (cron(0 5 ? * SUN *))
- `05:02:56` ✅   ✓ target → justhodl-symbol-dictionary
- `05:02:56` ✅   ✓ added invoke permission
## 3. Ledger count, post-race

- `05:02:56` ✗ ledger still empty despite 'emitted: 24' log
