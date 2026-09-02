# ops 5108 -- fix wave 2: erroring engines one by one

**Status:** failure  
**Duration:** 877.0s  
**Finished:** 2026-09-02T02:24:53+00:00  

## Error

```
SystemExit: 1
```

## Data

| errors | errors_sampled | exceptions | invoked | ports | requests | step | throttled | with_yoy |
|---|---|---|---|---|---|---|---|---|
|  | 1 |  |  |  |  | boj |  |  |
|  |  |  |  | 50 | 20 | portwatch | 0 | 0 |
|  |  | 0 |  |  |  | census-us |  |  |
|  |  | 0 |  |  |  | insider-trades |  |  |
| 0 |  |  | True |  |  | fi-census-sched |  |  |
| 0 |  |  | True |  |  | etf-census-sched |  |  |

## Log
## 1. boj-full storm

- `02:10:17` benzinga-news-agent-warm targets: [('1', 'benzinga-news-agent', ''), ('bojfanout', 'justhodl-boj-full', '{"fanout": true}'), ('econdispatch', 'justhodl-census-us', '{"mode": "econ_dispatch", "shards": 12}'), ('gdelt-full', 'justhodl-gdelt-full', '{}'), ('repo', 'justhodl-repo', '{}')]
- `02:10:17` ✅ detached boj-full target(s) ['bojfanout'] from the 5-minute warm rule
- `02:10:17` carry-surface-4h extra targets: [('censusts', 'justhodl-census-us')]
- `02:10:18` ✅ schedule justhodl-boj-full-fanout created (rate 30 minutes)
- `02:10:18`   boj err: [ERROR] NameError: name 'api_key' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 334, in lambda_handler
    ds = api_drain_db(db, end, state)
  File "/var/task/lambda_functi
## 2. memory/timeout raises (live)

- `02:10:19` ✅ justhodl-signal-scorecard: 256MB/120s -> 1536MB/600s
- `02:10:20` ✅ justhodl-feed-registry: 256MB/120s -> 1024MB/600s
- `02:10:20` ✅ justhodl-global-liquidity: 256MB/120s -> 512MB/300s
- `02:10:21` ✅ justhodl-provider-window-sentinel: 256MB/120s -> 512MB/600s
- `02:10:22` ✅ justhodl-research-backtest: 512MB/240s -> 1024MB/900s
- `02:10:22` ✅ justhodl-signal-harvester: 512MB/300s -> 1024MB/900s
- `02:10:23` ✅ justhodl-fleet-monitor: 512MB/300s -> 1024MB/900s
- `02:10:24` ✅ justhodl-calibrator: 512MB/300s -> 2048MB/900s
- `02:10:25` ✅ justhodl-import-sentinel: 512MB/120s -> 1024MB/600s
## 3. stock-screener redeploy

- `02:10:25`   zip: 122659 bytes
## 1. Lambda

- `02:10:25`   Lambda exists — updating
## 4. portwatch v1.6.1

- `02:13:30` ✅ justhodl-portwatch deployed (2026-09-02T02:13:19.000+0000) after 182s
- `02:20:40` portwatch invoke 200 b'{"ok": true, "chokepoints": 28, "worst": {"name": "Kerch Strait", "z": -1.48, "vs_baseline_pct": -98.9, "status": "DISRUPTED"}, "rows": 10948}'
- `02:20:40` portwatch: v1.6.1 ok=True chokepoints=28 ports=50 with_yoy=0 daily_rows=10948 requests={"n": 20, "throttled_429": 0, "budget": 140} history_through={"choke": "2026-08-23", "ports": "2025-10-16"} errors=["ports_daily: {'code': 400, 'message': 'Cannot perform query. Invalid query parameters.', 'details': ['Unable to perform query. Please check your parameters.']} (using history through 2025-10-16, 3000 new rows merged)"]
- `02:20:41` ✅ justhodl-portwatch-daily -> cron(20 11 * * ? *)
- `02:20:41` portwatch schedules now: ['justhodl-portwatch-daily']
## 5. census-us / insider-trades

- `02:20:41` ✅ justhodl-census-us deployed (2026-09-02T02:11:00.000+0000) after 0s
- `02:22:12` justhodl-census-us: UnboundLocalError lines after fix: 0 []; report: ["REPORT RequestId: 60611067-afcd-420d-ac4c-dd12ea09ca32\tDuration: 653.84 ms\tBilled Duration: 654 ms\tMemory Size: 1024 MB\tMax Memory Used: 121 MB\t\nXRAY TraceId: 1-6a97882b-5c621cc805b4099f014ceaf9\tSegmentId: 09217b2265f4d899\tSampled: true", "REPORT RequestId: c2b4065a-6fb7-40f4-b6b9-acc62641
- `02:22:13` ✅ justhodl-insider-trades deployed (2026-09-02T02:12:57.000+0000) after 0s
- `02:23:13` justhodl-insider-trades: KeyError lines after fix: 0 []; report: ["REPORT RequestId: 865cfc42-fd4e-4895-a3db-bb2c70c58b9a\tDuration: 18990.89 ms\tBilled Duration: 19344 ms\tMemory Size: 512 MB\tMax Memory Used: 113 MB\tInit Duration: 353.02 ms\t\nXRAY TraceId: 1-6a978855-079f9c826a040fb238ab7668\tSegmentId: 5c1408"]
## 6. timeout forensics

- `02:23:14`   justhodl-fleet-monitor: no 'Task timed out' in 3d
- `02:23:15`   justhodl-feed-registry: no 'Task timed out' in 3d
- `02:23:16`   justhodl-research-backtest: no 'Task timed out' in 3d
- `02:23:16`   justhodl-global-liquidity: no 'Task timed out' in 3d
- `02:23:17`   justhodl-provider-window-sentinel: no 'Task timed out' in 3d
- `02:23:18`   justhodl-signal-harvester: no 'Task timed out' in 3d
- `02:23:18`   justhodl-imf-full: no 'Task timed out' in 3d
- `02:23:19`   justhodl-calibrator: no 'Task timed out' in 3d
- `02:23:19`   justhodl-signal-scorecard: no 'Task timed out' in 3d
- `02:23:20`   justhodl-import-sentinel: no 'Task timed out' in 3d
- `02:23:20` import-sentinel error lines (24h):
- `02:23:20`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:23:20`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:23:20`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:23:20`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:23:20`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:23:20`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
## 7. fi-census / etf-census silent schedules

- `02:23:21` fi-census-sched: state=ENABLED expr=cron(0 7 2,16 * ? *) target=arn:aws:lambda:us-east-1:857687956942:function:justhodl-fi-census role=arn:aws:iam::857687956942:role/justhodl-scheduler-role input={} window={'Mode': 'OFF'}
- `02:23:21`   function arn=arn:aws:lambda:us-east-1:857687956942:function:justhodl-fi-census state=Active timeout=900 mem=768
- `02:23:21`   manual async invoke status=202
- `02:24:07`   report: ["REPORT RequestId: ea48f5bf-e2a9-47fe-8234-27dc1c8f00f9\tDuration: 20953.68 ms\tBilled Duration: 21375 ms\tMemory Size: 768 MB\tMax Memory Used: 108 MB\tInit Duration: 421.30 ms\t\nXRAY TraceId: 1-6a978899-556abc502b942e633de39267\tSegment errors: []
- `02:24:07` etf-census-sched: state=ENABLED expr=cron(30 6 2,16 * ? *) target=arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-census role=arn:aws:iam::857687956942:role/justhodl-scheduler-role input={} window={'Mode': 'OFF'}
- `02:24:08`   function arn=arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-census state=Active timeout=900 mem=1024
- `02:24:08`   manual async invoke status=202
- `02:24:53`   report: ["REPORT RequestId: 62e89396-4ef7-415b-bd84-360b9eaffb3a\tDuration: 23763.23 ms\tBilled Duration: 24259 ms\tMemory Size: 1024 MB\tMax Memory Used: 114 MB\tInit Duration: 494.77 ms\t\nXRAY TraceId: 1-6a9788c8-019678fb2e7628dc7362440d\tSegmen errors: []
## verdict

- `02:24:53` ✗ stock-screener: Parameter validation failed:
Invalid type for parameter Environment.Variables, value: None, type: <class 'NoneType'>, valid types: <class 'dict'>
- `02:24:53` ✗ portwatch: still no ports with yoy
