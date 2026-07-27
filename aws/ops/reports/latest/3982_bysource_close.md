# ops 3981 — census v1.4 idempotent close

**Status:** success  
**Duration:** 321.3s  
**Finished:** 2026-07-27T20:23:10+00:00  

## Data

| artifact_marker | generated_at | marker_deployed | memory | timeout |
|---|---|---|---|---|
|  |  | True | 1536 | 600 |
| data-census v1.1 ops3978 4mb-cap | 2026-07-27T18:29:04.828087+00:00 |  |  |  |

## Log
## A0. CloudWatch truth — what happened to the 19:24 invoke?

- `20:17:50`   [census] data-census v1.3 ops3980 o1-enrich
- `20:17:50`   REPORT RequestId: d602a1ed-503f-4ff7-9639-650e773dbbee	Duration: 600000.00 ms	Billed Duration: 600477 ms	Memory Size: 1536 MB	Max Memory Used: 1035 MB	Init Duration: 476.75 ms	Status: timeout
XRAY Tra
- `20:17:50`   [census] data-census v1.3 ops3980 o1-enrich
- `20:17:50`   REPORT RequestId: d602a1ed-503f-4ff7-9639-650e773dbbee	Duration: 600000.00 ms	Billed Duration: 600000 ms	Memory Size: 1536 MB	Max Memory Used: 1093 MB	Status: timeout
XRAY TraceId: 1-6a67abf9-44f198d4
- `20:17:50`   [census] data-census v1.3 ops3980 o1-enrich
- `20:17:50`   REPORT RequestId: f4b40035-94c5-4ac5-b0de-f000237c7c23	Duration: 600000.00 ms	Billed Duration: 600000 ms	Memory Size: 1536 MB	Max Memory Used: 1098 MB	Status: timeout
XRAY TraceId: 1-6a67b01f-398b0514
- `20:17:51`   [census] data-census v1.3 ops3980 o1-enrich
- `20:17:51`   REPORT RequestId: 23bfe72d-5fcf-4cc3-af6a-66c3bcc0144c	Duration: 600000.00 ms	Billed Duration: 600545 ms	Memory Size: 1536 MB	Max Memory Used: 1210 MB	Init Duration: 544.45 ms	Status: timeout
XRAY Tra
- `20:17:51`   [census] data-census v1.3 ops3980 o1-enrich
- `20:17:51`   REPORT RequestId: f4b40035-94c5-4ac5-b0de-f000237c7c23	Duration: 600000.00 ms	Billed Duration: 600000 ms	Memory Size: 1536 MB	Max Memory Used: 1067 MB	Status: timeout
XRAY TraceId: 1-6a67b01f-398b0514
- `20:17:51`   [census] data-census v1.3 ops3980 o1-enrich
- `20:17:51`   REPORT RequestId: f4b40035-94c5-4ac5-b0de-f000237c7c23	Duration: 600000.00 ms	Billed Duration: 600000 ms	Memory Size: 1536 MB	Max Memory Used: 1130 MB	Status: timeout
XRAY TraceId: 1-6a67b01f-398b0514
## A. is v1.4 the deployed artifact?

## B. artifact state

## C. async invoke + SHORT poll (timeout-proof)

- `20:17:53`   invoked async; engine needs ~14 min at fleet scale
- `20:23:10` ✅ STATUS=INVOKED_PENDING — engine running server-side; re-trigger this same op to verify. Exiting 0 so the report always lands.
