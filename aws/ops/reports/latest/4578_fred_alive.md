# ops 4578 — FRED alive after the s3 one-liner

**Status:** failure  
**Duration:** 502.5s  
**Finished:** 2026-08-10T04:24:49+00:00  

## Error

```
SystemExit: 1
```

## Data

| _justhodl_fred-api-key | _justhodl_fred_api-key | env.FRED_API_KEY | gates_failed | throttles_10m_before |
|---|---|---|---|---|
| 32 | 32 | 32 |  |  |
|  |  |  |  | 42 |
|  |  |  | 1 |  |

## Log
## 1. Settle the v2.2.1 deploy

- `04:16:27` ✅ settled: LastModified 2026-08-10T04:15:50.000+0000
## 2. Key belts (runner view)

## 3. Purge the crash-loop retry backlog

- `04:17:58` ✅ backlog purged (age60/retries0 for 90s, then restored)
## 4. The lambda's honest answer

- `04:17:58`   slot busy (1/8) — waiting 18s
- `04:18:17`   slot busy (2/8) — waiting 18s
- `04:18:35`   slot busy (3/8) — waiting 18s
- `04:18:54`   slot busy (4/8) — waiting 18s
- `04:19:13`   slot busy (5/8) — waiting 18s
- `04:19:31`   slot busy (6/8) — waiting 18s
- `04:19:50`   slot busy (7/8) — waiting 18s
- `04:20:09`   slot busy (8/8) — waiting 18s
- `04:20:27` ✗ slot never freed — crash-loop still alive; read the log tail below
## 5. Checkpoint proof (200s window)

- `04:23:48` ✅ state moving: 2026-08-10T04:15:40+00:00 → 2026-08-10T04:15:40+00:00 | status=walking | imported=12246 | cursor=382/None | rpm=88.0 | scope=scoped_7_roots | v=None
## 6. Log tail (ground truth)

- `04:23:48`   | INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3
- `04:23:48`   | START RequestId: fb69b7fe-b405-455c-b7d8-288408970be0 Version: $LATEST
- `04:23:48`   | END RequestId: fb69b7fe-b405-455c-b7d8-288408970be0
- `04:23:48`   | REPORT RequestId: fb69b7fe-b405-455c-b7d8-288408970be0	Duration: 246.84 ms	Billed Duration: 247 ms	Memory Size: 2048 MB	Max Memory Used: 100 MB	
XRAY 
- `04:23:48`   | START RequestId: 1249f0b7-b08e-4e2d-a21d-9dc5b4a075ce Version: $LATEST
- `04:23:48`   | [ERROR] KeyError: 'categories_done'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 707, in lambda_handler
    print(js
- `04:23:48`   | END RequestId: 1249f0b7-b08e-4e2d-a21d-9dc5b4a075ce
- `04:23:48`   | REPORT RequestId: 1249f0b7-b08e-4e2d-a21d-9dc5b4a075ce	Duration: 227.10 ms	Billed Duration: 228 ms	Memory Size: 2048 MB	Max Memory Used: 100 MB	
XRAY 
- `04:23:48`   | START RequestId: 31260646-6997-4743-95a0-31b032d4d0d9 Version: $LATEST
- `04:23:48`   | [ERROR] KeyError: 'categories_done'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 707, in lambda_handler
    print(js
- `04:23:48`   | END RequestId: 31260646-6997-4743-95a0-31b032d4d0d9
- `04:23:48`   | REPORT RequestId: 31260646-6997-4743-95a0-31b032d4d0d9	Duration: 266.55 ms	Billed Duration: 267 ms	Memory Size: 2048 MB	Max Memory Used: 100 MB	
XRAY 
## 7. Storm gate

- `04:24:49` ⚠ Throttles(5m)=9 — settling; sentinel keeps watch
## VERDICT

- `04:24:49` ✗ 1 gate(s) failed
