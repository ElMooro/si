# ops 3199 — diagnose the dead runner, patch, prove

**Status:** failure  
**Duration:** 721.6s  
**Finished:** 2026-07-13T03:49:33+00:00  

## Error

```
SystemExit: 1
```

## Data

| last_modified | memory | n_fails | n_warns | state | timeout | verdict |
|---|---|---|---|---|---|---|
| 2026-07-13T03:15:53 | 3008 |  |  | Active | 900 |  |
|  |  | 1 | 0 |  |  | FAIL |

## Log
## 1. Evidence: config + CloudWatch tail

- `03:37:32`   [wl] 207 engines (160 ACTIVE) · 3759 unique series
- `03:37:32`   [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278
- `03:37:32`   REPORT RequestId: cdb3a88d-1538-46f2-ab44-991d408ca03c	Duration: 2805.59 ms	Billed Duration: 3324 ms	Memory Size: 3008 MB	Max Memory Used: 233 MB	Init
- `03:37:32`   [wl] 207 engines (160 ACTIVE) · 3759 unique series
- `03:37:32`   [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278
- `03:37:32`   REPORT RequestId: cdb3a88d-1538-46f2-ab44-991d408ca03c	Duration: 2663.45 ms	Billed Duration: 2664 ms	Memory Size: 3008 MB	Max Memory Used: 260 MB	
XRA
- `03:37:32`   [wl] 207 engines (160 ACTIVE) · 3759 unique series
- `03:37:32`   [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278
- `03:37:32`   REPORT RequestId: cdb3a88d-1538-46f2-ab44-991d408ca03c	Duration: 2630.99 ms	Billed Duration: 2631 ms	Memory Size: 3008 MB	Max Memory Used: 260 MB	
XRA
- `03:37:32`   [wl] 207 engines (160 ACTIVE) · 3759 unique series
- `03:37:32`   [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278
- `03:37:32`   REPORT RequestId: 92d154f0-556f-4c84-9b5d-35354ced1e5e	Duration: 2899.60 ms	Billed Duration: 3455 ms	Memory Size: 3008 MB	Max Memory Used: 233 MB	Init
- `03:37:32`   [wl] 207 engines (160 ACTIVE) · 3759 unique series
- `03:37:32`   [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278
- `03:37:32`   REPORT RequestId: 92d154f0-556f-4c84-9b5d-35354ced1e5e	Duration: 2987.30 ms	Billed Duration: 2988 ms	Memory Size: 3008 MB	Max Memory Used: 260 MB	
XRA
- `03:37:32`   [wl] 207 engines (160 ACTIVE) · 3759 unique series
- `03:37:32`   [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278
- `03:37:32`   REPORT RequestId: 92d154f0-556f-4c84-9b5d-35354ced1e5e	Duration: 2809.42 ms	Billed Duration: 3333 ms	Memory Size: 3008 MB	Max Memory Used: 234 MB	Init
## 2. Redeploy patched shared bundle

- `03:37:32`   zip: 77142 bytes
## 1. Lambda

- `03:37:33`   Lambda exists — updating
- `03:37:36` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `03:37:36`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `03:37:36` ✅   ✓ target → justhodl-wl-engines
- `03:37:36` ✅   ✓ added invoke permission
- `03:37:36`   zip: 78761 bytes
## 1. Lambda

- `03:37:36`   Lambda exists — updating
- `03:37:41` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `03:37:42`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `03:37:42` ✅   ✓ target → justhodl-thesis-engine
- `03:37:42` ✅   ✓ added invoke permission
- `03:37:42`   zip: 74802 bytes
## 1. Lambda

- `03:37:42`   Lambda exists — updating
- `03:37:45` ✅   ✓ updated justhodl-symbol-dictionary
## 2. EB rule + permissions

- `03:37:45`   rule already correct: symbol-dictionary-weekly (cron(0 5 ? * SUN *))
- `03:37:45` ✅   ✓ target → justhodl-symbol-dictionary
- `03:37:45` ✅   ✓ added invoke permission
## 3. Re-run + fresh-index gate

- `03:49:33`   tail: INIT_START Runtime Version: python:3.12.mainlinev2.v14	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:40182b778d40c8bdb13a6ef86990df74f5066cdb
- `03:49:33`   tail: START RequestId: ab77e94d-2071-41f0-840e-87e7db847c2e Version: $LATEST

- `03:49:33`   tail: [wl] 207 engines (160 ACTIVE) · 3759 unique series

- `03:49:33`   tail: [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278
- `03:49:33`   tail: END RequestId: ab77e94d-2071-41f0-840e-87e7db847c2e

- `03:49:33`   tail: REPORT RequestId: ab77e94d-2071-41f0-840e-87e7db847c2e	Duration: 2869.45 ms	Billed Duration: 3410 ms	Memory Size: 3008 MB	Max Memory Used: 233 MB	Init
- `03:49:33`   tail: START RequestId: ab77e94d-2071-41f0-840e-87e7db847c2e Version: $LATEST

- `03:49:33`   tail: [wl] 207 engines (160 ACTIVE) · 3759 unique series

- `03:49:33`   tail: [ERROR] ValueError: not enough values to unpack (expected 3, got 2)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 278
- `03:49:33`   tail: END RequestId: ab77e94d-2071-41f0-840e-87e7db847c2e

- `03:49:33`   tail: REPORT RequestId: ab77e94d-2071-41f0-840e-87e7db847c2e	Duration: 2723.45 ms	Billed Duration: 2724 ms	Memory Size: 3008 MB	Max Memory Used: 260 MB	
XRA
- `03:49:33` ✗ index STILL stale after patched re-run (generated_at=2026-07-13T02:46:02.447903+00:00)
