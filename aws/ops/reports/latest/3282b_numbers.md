## 0. Why the last two invokes wrote nothing

**Status:** failure  
**Duration:** 944.4s  
**Finished:** 2026-07-14T02:07:23+00:00  

## Error

```
SystemExit: 1
```

## Data

| n_fails | n_warns | verdict |
|---|---|---|
| 1 | 0 | FAIL |

## Log
- `01:51:41`   [13f] resolver: 150 fresh lookups, 4197 mapped
- `01:51:41`   [ERROR] KeyError: 'put_funds'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 1015, in lambda_handler
    by_ticker = a
- `01:51:41`   [13f] resolver: 150 fresh lookups, 4242 mapped
- `01:51:41`   [13f] resolver: 150 fresh lookups, 4291 mapped
- `01:51:41`   [13f] resolver: 150 fresh lookups, 4341 mapped
- `01:51:41`   [ERROR] KeyError: 'put_funds'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 1171, in lambda_handler
    by_ticker = a
- `01:51:41`   [13f] resolver: 150 fresh lookups, 4384 mapped
- `01:51:41`   [13f] resolver: 150 fresh lookups, 4442 mapped
- `01:51:41`   [13f] resolver: 150 fresh lookups, 4506 mapped
- `01:51:41`   [ERROR] KeyError: 'put_funds'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 1181, in lambda_handler
    by_ticker = a
- `01:51:41`   zip: 91731 bytes
## 1. Lambda

- `01:51:41`   Lambda exists — updating
- `01:51:46` ✅   ✓ updated justhodl-13f-positions
## 1. THE QUARTER'S NUMBERS

- `02:07:23` ✗ feed STILL not fresh — read logs above
