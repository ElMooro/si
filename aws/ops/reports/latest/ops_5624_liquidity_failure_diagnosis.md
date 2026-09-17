# Liquidity runtime failure location

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-09-17T16:38:44+00:00  

## Data

| error_type | frames |
|---|---|
| IndexError | ['File "/var/task/lambda_function.py", line 873, in lambda_handler', 'File "/var/task/lambda_function.py", line 615, in build_part4', 'File "/var/task/lambda_function.py", line 259, in get_series_history', 'File "/var/task/lambda_function.py", line 233, in fetch_fred', 'File "/var/task/lambda_function.py", line 233, in <genexpr>'] |

## Log
- `16:38:44` ✅ Read-only error locations collected
