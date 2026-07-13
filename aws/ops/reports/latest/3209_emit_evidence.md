# ops 3209 — emission failure named from the logs

**Status:** success  
**Duration:** 19.7s  
**Finished:** 2026-07-13T04:59:55+00:00  

## Data

| checker_status | function_error | markers | n_fails | n_warns | verdict |
|---|---|---|---|---|---|
| 200 | none |  |  |  |  |
|  |  | 3 | 0 | 0 | PASS |

## Log
## 1. Runner log markers

- `04:59:35`   [wl] trust-ledger signals emitted: 24
- `04:59:36`   [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278, in lambda_handler

- `04:59:36`   [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278, in lambda_handler

## 2. Checker retry (was TooManyRequests)

