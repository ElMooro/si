# ops 6762 - why justhodl-leaders did not write on Sunday 2026-11-08 (Scheduler state, 4-day metrics, last log streams; strength as the control; read-only)

**Status:** success  
**Duration:** 3.4s  
**Finished:** 2026-11-08T11:23:13+00:00  

## Data

| leaders_age_h | leaders_error_lines | leaders_metrics | leaders_schedule | leaders_timeout_s | strength_age_h | strength_metrics | strength_schedule | strength_timeout_s |
|---|---|---|---|---|---|---|---|---|
| 31.5 | 1 | 11-04 n1 e0 t0 dmax22s, 11-05 n1 e0 t0 dmax30s, 11-06 n1 e0 t0 dmax31s, 11-07 n3 e3 t0 dmax1s | ENABLED cron(50 3 * * ? *) | 300 | 7.7 | 11-04 n4 e0 t0 dmax19s, 11-05 n6 e0 t0 dmax29s, 11-06 n1 e0 t0 dmax25s, 11-07 n1 e0 t0 dmax30s | ENABLED cron(40 3 * * ? *) | 300 |

## Log
## justhodl-leaders

- `11:23:10` data/leaders.json: 31.5 h old (3087200 B), last modified 2026-11-07T03:50:39+00:00
- `11:23:10` function: state Active / Successful, timeout 300s, memory 2048 MB, last modified 2026-11-05T08:14:28.000+0000, runtime python3.12
- `11:23:10` schedule justhodl-leaders-daily: ENABLED cron(50 3 * * ? *) (tz UTC) | target justhodl-leaders | FTW OFF
- `11:23:11` metrics per UTC day: 11-04 n1 e0 t0 dmax22s, 11-05 n1 e0 t0 dmax30s, 11-06 n1 e0 t0 dmax31s, 11-07 n3 e3 t0 dmax1s
- `11:23:11` stream 473a8b52693c42ddce80: last event 2026-11-08T03:53+00:00, 13 events, last invocation 4 lines
- `11:23:11`   11-08 03:53:09 START RequestId: 496aeff1-68c3-49dd-a522-a58732c33fc3 Version: $LATEST
- `11:23:11`   11-08 03:53:09 [ERROR] RuntimeError: only 58 leaders from 27109 factor-book rows: the book is short; the previous publication stands Traceback (most recent call last): File "/var/task/lambda_function.py", line 1106, in lambda_handler out = build(s3, now, remaining_s=remaining) File "/var/task/lambda_function.py", line 838, in build raise RuntimeError("only %d leaders from %d factor-book rows: the book is short; the previous publication stands" % (len(leaders), len(book)))
- `11:23:11`   11-08 03:53:09 END RequestId: 496aeff1-68c3-49dd-a522-a58732c33fc3
- `11:23:11`   11-08 03:53:09 REPORT RequestId: 496aeff1-68c3-49dd-a522-a58732c33fc3 Duration: 143.14 ms Billed Duration: 144 ms Memory Size: 2048 MB Max Memory Used: 138 MB XRAY TraceId: 1-6aeff16f-60ef9ca5632831e05efd58df SegmentId: b1d70fb3acf3e120 Sampled: true
- `11:23:12` stream 4ee6bc73053abd50ffaa: last event 2026-11-07T03:50+00:00, 8 events, last invocation 7 lines
- `11:23:12`   11-07 03:50:07 START RequestId: 496aee9f-e8c3-49dd-a522-a58732c33fc3 Version: $LATEST
- `11:23:12`   11-07 03:50:09 leaders: 673 leaders in 145 industries (27112 factor-book rows, 5329 common, 73 classes folded), 109 funds, bank last session 2026-11-06
- `11:23:12`   11-07 03:50:15 [WARNING] 2026-11-07T03:50:15.690Z 496aee9f-e8c3-49dd-a522-a58732c33fc3 Connection pool is full, discarding connection: justhodl-dashboard-live.s3.amazonaws.com. Connection pool size: 10
- `11:23:12`   11-07 03:50:15 [WARNING] 2026-11-07T03:50:15.696Z 496aee9f-e8c3-49dd-a522-a58732c33fc3 Connection pool is full, discarding connection: justhodl-dashboard-live.s3.amazonaws.com. Connection pool size: 10
- `11:23:12`   11-07 03:50:38 leaders: 673 leaders / 145 industries / 109 funds as of 2026-11-06; beating SPY 1M 76.2%; A/D 198/257; smart 145/171; golden 20 death 84; dtop 68 dbot 57; climax 20/26; tight 36; flows 109/109; 3087200 bytes, 30.3s, errors 0
- `11:23:12`   11-07 03:50:38 END RequestId: 496aee9f-e8c3-49dd-a522-a58732c33fc3
- `11:23:12`   11-07 03:50:38 REPORT RequestId: 496aee9f-e8c3-49dd-a522-a58732c33fc3 Duration: 30733.94 ms Billed Duration: 31335 ms Memory Size: 2048 MB Max Memory Used: 774 MB Init Duration: 600.89 ms XRAY TraceId: 1-6aee9fef-48ef49a762f5aa60680f8cd5 SegmentId: 1a9ce402e594b7c2 Sampled: true
## justhodl-strength

- `11:23:12` data/strength.json: 7.7 h old (3645264 B), last modified 2026-11-08T03:40:40+00:00
- `11:23:12` function: state Active / Successful, timeout 300s, memory 2048 MB, last modified 2026-11-06T03:57:01.000+0000, runtime python3.12
- `11:23:12` schedule justhodl-strength-daily: ENABLED cron(40 3 * * ? *) (tz UTC) | target justhodl-strength | FTW OFF
- `11:23:12` metrics per UTC day: 11-04 n4 e0 t0 dmax19s, 11-05 n6 e0 t0 dmax29s, 11-06 n1 e0 t0 dmax25s, 11-07 n1 e0 t0 dmax30s
- `11:23:13` stream 41a79d75e9e9feb40fed: last event 2026-11-08T03:40+00:00, 10 events, last invocation 9 lines
- `11:23:13`   11-08 03:40:09 START RequestId: 476aefef-1038-4f0a-9456-920bf93967ed Version: $LATEST
- `11:23:13`   11-08 03:40:10 strength: universe 853 (desk 163, etf-desk 116, sp500 503, 13f 300), bank last session 2026-11-07
- `11:23:13`   11-08 03:40:27 [WARNING] 2026-11-08T03:40:27.574Z 476aefef-1038-4f0a-9456-920bf93967ed Connection pool is full, discarding connection: justhodl-dashboard-live.s3.amazonaws.com. Connection pool size: 10
- `11:23:13`   11-08 03:40:27 [WARNING] 2026-11-08T03:40:27.575Z 476aefef-1038-4f0a-9456-920bf93967ed Connection pool is full, discarding connection: justhodl-dashboard-live.s3.amazonaws.com. Connection pool size: 10
- `11:23:13`   11-08 03:40:39 strength: thesis 252 sessions, 27 dump days, 23 rally days, 8.0s
- `11:23:13`   11-08 03:40:39 strength: history data/strength/history/2026-11-07.json.gz (506589 bytes gz)
- `11:23:13`   11-08 03:40:39 strength: 852 rows (18 ranked), as of 2026-11-07, leaders ['NDAQ', 'FIVE', 'LULU', 'OZK', 'NTRS'], coil ['NTRS'], flows current 131/131, 13f 735, classes {"stock": 666, "equity_etf": 48, "country": 37, "bond": 14, "high_yield": 11, "cash": 2, "gold": 2, "silver": 2, "metals": 13, "commodity": 10, "currency": 9, "crypto": 8, "volatility": 2, "leveraged": 0}, industries 76, thesis last 2026-11-06 spy 0.31 momentum-top-tenth held 25.8% excess -1.53 pp (63-session dump days 5, rally days 4), 3645264 bytes, 29.4s, errors 0
- `11:23:13`   11-08 03:40:39 END RequestId: 476aefef-1038-4f0a-9456-920bf93967ed
- `11:23:13`   11-08 03:40:39 REPORT RequestId: 476aefef-1038-4f0a-9456-920bf93967ed Duration: 30151.29 ms Billed Duration: 30647 ms Memory Size: 2048 MB Max Memory Used: 849 MB Init Duration: 494.74 ms XRAY TraceId: 1-6aefef19-6b24db0677acc6d406930c5d SegmentId: f46503bc657ce640 Sampled: true
- `11:23:13` stream 4d1cab404d646e84208d: last event 2026-11-07T03:43+00:00, 1 events, last invocation 1 lines
- `11:23:13`   11-07 03:43:13 INIT_START Runtime Version: python:3.12.mainlinev2.v82 Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:***
- `11:23:13` ✅ read-only; nothing invoked, nothing changed
