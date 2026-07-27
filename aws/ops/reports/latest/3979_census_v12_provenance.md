# ops 3977 — data-census async close + schedule + page edge

**Status:** failure  
**Duration:** 920.5s  
**Finished:** 2026-07-27T19:00:05+00:00  

## Error

```
SystemExit: 1
```

## Data

| memory | timeout | v12_settled |
|---|---|---|
| 1536 | 600 | True |

## Log
## A0. the truth from CloudWatch

- `18:44:45`   [census] data-census v1.1 ops3978 4mb-cap
- `18:44:45`   [census] DONE 839.7s arts=10022 paths=729243 mislabels=0 conflicts=0 gaps=0
- `18:44:45`   REPORT RequestId: 348d73b5-5b96-4731-bc3c-8607a2ce6375	Duration: 842179.94 ms	Billed Duration: 842752 ms	Memory Size: 3008 MB	Max Memory Used: 697 MB	Init Duration: 571.82 ms	
XRAY TraceId: 
- `18:44:45`   [census] data-census v1.1 ops3978 4mb-cap
- `18:44:45`   [census] DONE 840.3s arts=10015 paths=726786 mislabels=0 conflicts=0 gaps=0
- `18:44:45`   REPORT RequestId: 70dcee71-2916-4f8a-8360-414730bce0a1	Duration: 842718.84 ms	Billed Duration: 843218 ms	Memory Size: 3008 MB	Max Memory Used: 686 MB	Init Duration: 498.34 ms	
XRAY TraceId: 
## A1. push engine v1.2 + settle by marker

## A. does the crashed invoke's output already exist?

- `18:44:57`   found generated_at=2026-07-27T18:29:04.828087+00:00 fresh=False
## B. async invoke + poll (the 3972 pattern)

- `18:46:58`   [5] still waiting
- `18:48:59`   [11] still waiting
- `18:51:00`   [17] still waiting
- `18:53:01`   [23] still waiting
- `18:55:02`   [29] still waiting
- `18:57:03`   [35] still waiting
- `18:59:04`   [41] still waiting
- `19:00:05` ✗ census never wrote — check CloudWatch for the lambda error
