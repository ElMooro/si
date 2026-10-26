# ops 6660 - justhodl-bank-factors first-join log probe (read-only)

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-10-26T04:07:55+00:00  

## Log
## 1. REPORT lines (duration / memory) and runtime errors

- `04:07:54` pattern "REPORT RequestId" -> 2 events
- `04:07:54`    03:47:53 | REPORT RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f	Duration: 18913.07 ms	Billed Duration: 19450 ms	Memory Size: 3008 MB	Max Memory Used: 3008 MB	Init Duration: 536.73 ms	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adecd55-21b35d3f4cc86af857e656aa	SegmentId: cef700ad7b3eac13	Sampled: true
- `04:07:54`    03:49:00 | REPORT RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f	Duration: 573.79 ms	Billed Duration: 574 ms	Memory Size: 3008 MB	Max Memory Used: 92 MB	 ⏎ XRAY TraceId: 1-6adecd55-21b35d3f4cc86af857e656aa	SegmentId: d9aa7132ca7f2f38	Sampled: true
- `04:07:54` pattern ?Traceback ?"Task timed out" ?"Runtime.O -> 1 events
- `04:07:54`    03:47:53 | REPORT RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f	Duration: 18913.07 ms	Billed Duration: 19450 ms	Memory Size: 3008 MB	Max Memory Used: 3008 MB	Init Duration: 536.73 ms	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adecd55-21b35d3f4cc86af857e656aa	SegmentId: cef700ad7b3eac13	Sampled: true
## 2. engine progress lines

- `04:07:54`    03:47:34 | [bank-factors] v1.0.0 start; budget 840s
## 3. raw tail (last 40 events, any stream)

- `04:07:54` stream $LATEST]60763207f4824dd092feda6e8aeb6b07 (8 events shown)
- `04:07:54`    03:47:34 | INIT_START Runtime Version: python:3.12.mainlinev2.v43	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:36aeb19f3326e129d9f096dea0a3c6a9ca8dce2610e62d2b2060747602d863ae
- `04:07:54`    03:47:34 | START RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f Version: $LATEST
- `04:07:54`    03:47:34 | [bank-factors] v1.0.0 start; budget 840s
- `04:07:54`    03:47:53 | END RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f
- `04:07:54`    03:47:53 | REPORT RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f	Duration: 18913.07 ms	Billed Duration: 19450 ms	Memory Size: 3008 MB	Max Memory Used: 3008 MB	Init Duration: 536.73 ms	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adecd55-21b35d3f4cc86af857e656aa	SegmentId: cef700ad7b3eac13	Sampled: true
- `04:07:54`    03:49:00 | START RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f Version: $LATEST
- `04:07:54`    03:49:00 | END RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f
- `04:07:54`    03:49:00 | REPORT RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f	Duration: 573.79 ms	Billed Duration: 574 ms	Memory Size: 3008 MB	Max Memory Used: 92 MB	 ⏎ XRAY TraceId: 1-6adecd55-21b35d3f4cc86af857e656aa	SegmentId: d9aa7132ca7f2f38	Sampled: true
## 4. state / manifest objects

- `04:07:55` data/warm/factor-book/_state/state.json: {"lease_until": 1792987454.655884}
- `04:07:55` data/warm/factor-book/manifest.json: absent (NoSuchKey)
- `04:07:55` symbols/ prefix: 0 object(s) in the first page
- `04:07:55` ✅ ops 6660 complete (read-only)
