# ops 4597 — combined drain rate (ceiling 100 x threaded banking)

**Status:** failure  
**Duration:** 804.1s  
**Finished:** 2026-08-11T00:32:08+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Settle deploy, roll past the old-code lease

- `00:18:45`   lease_until in 791s (old-code invoke may hold it)
- `00:24:05`   chain already running — will pick up new code on its next self-invoke
## 2. Seven-minute banked-delta measurement

- `00:25:06`   t0: imported=60178 cursor=53188 rpm=100.0 ver=2.2.1
- `00:32:06`   t1: imported=60638 cursor=53648 rpm=100.0 ver=2.2.1
- `00:32:06`   measured: +460 series in 7 min = 65.7/min (3943/h)
## 3. Contracts + honest ETA

- `00:32:06` ✗   [drain] CONTRACT MISS — v2.3 live in state (ver=2.2.1)
- `00:32:06` ✅   [drain] no 403/key block (blocked_at=None status=walking)
- `00:32:06` ✅   [drain] threaded banking clean (0 bank_put errors)
- `00:32:06` ✅   [drain] rate 65.7/min beats the serial-era 49/min (target ~85+)
- `00:32:07`   remaining=221457 → ETA 56.2 h (~2.3 days), finish ≈ 2026-08-13 08:42 UTC
## 4. 4592 stragglers — post-invoke CloudWatch truth

- `00:32:08`   justhodl-catalyst-skew-premove: 13 lines last-60m, 0 death-sigs
- `00:32:08`     | START RequestId: 644f25b7-f2c8-4616-aee2-f243b2a18f8b Version: $LATEST
- `00:32:08`     | END RequestId: 644f25b7-f2c8-4616-aee2-f243b2a18f8b
- `00:32:08`     | REPORT RequestId: 644f25b7-f2c8-4616-aee2-f243b2a18f8b	Duration: 1655.10 ms	Billed Duration: 1978 ms	Memory Size: 256 MB	Max Memory Used: 97 MB	Init Duration: 322.04 ms	
XRAY TraceId: 1-6a7a67c2-0701d869329bf462748bc57c	SegmentId:
- `00:32:08`     | INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `00:32:08`   justhodl-failed-pattern-reversal: 14 lines last-60m, 0 death-sigs
- `00:32:08`     | START RequestId: 81448d94-0afe-4e8f-bffb-a6ce35d62705 Version: $LATEST
- `00:32:08`     | universe size: 0
- `00:32:08`     | END RequestId: 81448d94-0afe-4e8f-bffb-a6ce35d62705
- `00:32:08`     | REPORT RequestId: 81448d94-0afe-4e8f-bffb-a6ce35d62705	Duration: 137.74 ms	Billed Duration: 138 ms	Memory Size: 1024 MB	Max Memory Used: 103 MB	
XRAY TraceId: 1-6a7a67ca-101a753b4f812c2c66ea736c	SegmentId: 36e48fcde46afc80	Sampled
## verdict

- `00:32:08` ✗ drain rate: 1 red
