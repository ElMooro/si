# ops 4573 — v2 liveness diagnosis

**Status:** success  
**Duration:** 1.0s  
**Finished:** 2026-08-10T00:30:55+00:00  

## Data

| blocked_at | cats_done_n | errors | invocations | last_pop_drained | lease_in_s | lease_until | next_popularity | phase2 | queue_cursor | queue_total | rate_rpm | series_imported | status | throttled_429 | throttles | updated_at |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None | 81 |  |  | None | -1786321854.6 | 0 | None | None | None | None | None | 11927 | walking | None |  | 2026-08-10T00:08:47+00:00 |
|  |  | 4 | 21 |  |  |  |  |  |  |  |  |  |  |  | 105 |  |

## Log
- `00:30:54` ⚠ queue ledger object absent
## log tail (filtered)

- `00:30:55` {"categories_done": 0, "series_seen": 0, "series_excluded_stale": 0, "series_imported": 0, "status": "paused"}
- `00:30:55` REPORT RequestId: 57731f45-ff52-4669-a68f-53fc94c301c4	Duration: 30.31 ms	Billed Duration: 565 ms	Memory Size: 2048 MB	Max Memory Used: 97 MB	Init Duration: 533.77 ms	
XRAY TraceId: 1-6a7916ea-4e7e08f26b3a1bb94411ad0f	Se
- `00:30:55` [ERROR] KeyError: 'categories_done'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 518, in lambda_handler
    print(json.dumps({k: m[k] for k in
- `00:30:55` REPORT RequestId: 2b56630c-803b-4350-8f89-947124f4fc76	Duration: 28.77 ms	Billed Duration: 29 ms	Memory Size: 2048 MB	Max Memory Used: 97 MB	
XRAY TraceId: 1-6a7916ec-3ef775830aff0b7543a87313	SegmentId: 7113798ac3168967	
- `00:30:55` [ERROR] KeyError: 'categories_done'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 518, in lambda_handler
    print(json.dumps({k: m[k] for k in
- `00:30:55` REPORT RequestId: 2b56630c-803b-4350-8f89-947124f4fc76	Duration: 41.66 ms	Billed Duration: 42 ms	Memory Size: 2048 MB	Max Memory Used: 97 MB	
XRAY TraceId: 1-6a7916ec-3ef775830aff0b7543a87313	SegmentId: 85ecd013c928a12e	
- `00:30:55` [ERROR] KeyError: 'categories_done'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 518, in lambda_handler
    print(json.dumps({k: m[k] for k in
- `00:30:55` REPORT RequestId: 2b56630c-803b-4350-8f89-947124f4fc76	Duration: 43.14 ms	Billed Duration: 44 ms	Memory Size: 2048 MB	Max Memory Used: 97 MB	
XRAY TraceId: 1-6a7916ec-3ef775830aff0b7543a87313	SegmentId: 1af45107a171abc1	
- `00:30:55` [ERROR] NameError: name 's3' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 559, in lambda_handler
    s3.put_object(Bucket=BUCKET,
- `00:30:55` REPORT RequestId: d360ee76-3bcf-4dff-9bea-cce2209c9de6	Duration: 780201.82 ms	Billed Duration: 780202 ms	Memory Size: 2048 MB	Max Memory Used: 99 MB	
XRAY TraceId: 1-6a7917bb-1116698f724125cd5c12b46e	SegmentId: 668a182db
- `00:30:55` REPORT RequestId: 88108819-395c-4ca0-9584-486ee8a0bcac	Duration: 1.50 ms	Billed Duration: 2 ms	Memory Size: 2048 MB	Max Memory Used: 99 MB	
XRAY TraceId: 1-6a7917bb-2fdf45e62595c1c961f5ba57	SegmentId: 3d6d4990349839a2	Sa
- `00:30:55` REPORT RequestId: 4f6b90c3-717e-4e00-a6b1-cfcc6b26de04	Duration: 492.28 ms	Billed Duration: 1045 ms	Memory Size: 768 MB	Max Memory Used: 99 MB	Init Duration: 552.13 ms	
XRAY TraceId: 1-6a7911df-7e6b824b08ea00e55f5c3a5f	S
- `00:30:55` REPORT RequestId: 5d8aad0a-e841-4643-a338-1803012c59d4	Duration: 154.59 ms	Billed Duration: 155 ms	Memory Size: 768 MB	Max Memory Used: 100 MB	
XRAY TraceId: 1-6a79130b-46f2f24e462d36a21e238a54	SegmentId: 8c5914172fca9f0
- `00:30:55` {"categories_done": 81, "series_seen": 531149, "series_excluded_stale": 163, "series_imported": 11791, "status": "walking"}
- `00:30:55` REPORT RequestId: d9c71f09-270f-4d14-b3c7-d7bb94d3094a	Duration: 190489.34 ms	Billed Duration: 190490 ms	Memory Size: 768 MB	Max Memory Used: 122 MB	
XRAY TraceId: 1-6a791437-2187152b7f4fe2176f009c01	SegmentId: 68bd2ae14
- `00:30:55` REPORT RequestId: b19e0754-3d79-4edb-92bd-e0cfafedc176	Duration: 172.40 ms	Billed Duration: 173 ms	Memory Size: 768 MB	Max Memory Used: 122 MB	
XRAY TraceId: 1-6a791563-0eae4e0609381b113b449caa	SegmentId: 0eb79380cc0e4da
- `00:30:55` REPORT RequestId: 72f2198e-9d75-437c-927a-e70ff1fd27cf	Duration: 851.51 ms	Billed Duration: 852 ms	Memory Size: 768 MB	Max Memory Used: 122 MB	
XRAY TraceId: 1-6a79168f-339f3b9a0038e1a568e5cff2	SegmentId: 5ddc994e8b2fcf6
- `00:30:55` ✅ verdict: V2_CRASHING
