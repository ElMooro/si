## P0 the ceiling

**Status:** success  
**Duration:** 544.2s  
**Finished:** 2026-08-31T15:51:19+00:00  

## Data

| boj | census_econ | census_moved | limit | throttles_12h |
|---|---|---|---|---|
| 60725 | 677 | False | 1000 | 35323 |

## Log
- `15:42:15`   ConcurrentExecutions limit : 1000
- `15:42:15`   UnreservedConcurrentExecutions: 783
- `15:42:15`   functions=871  code storage 0.1 GB of 322.1 GB
- `15:42:15`   Throttles              03=3215 04=3151 05=3073 06=2930 07=3176 08=2801 09=2826 10=2904 11=2984 12=2892 13=2557 14=2814
- `15:42:16`   Invocations            03=1235 04=1310 05=1292 06=1295 07=1466 08=1354 09=1289 10=1318 11=1356 12=1457 13=1451 14=1407
- `15:42:16`   ConcurrentExecutions   03=15 04=26 05=16 06=16 07=29 08=25 09=15 10=26 11=17 12=28 13=19 14=18
- `15:42:16`   fleet-wide throttles in 12h: 35323
- `15:42:16`   -> invokes ARE being refused. More shards would be
- `15:42:16`      converted into TooManyRequests, not into data.
## P1 is the check really in the deployed package

- `15:42:16`   deployed LastModified=2026-08-31T02:02:25.000+0000
- `15:42:16`   package 106,913 bytes, lambda_function.py 21,338 bytes
- `15:42:16`   contains DEAD_LANE_H    : True
- `15:42:16`   contains dead-lanes     : True
- `15:42:16`   contains _state/        : True
- `15:42:16`   context: ...ambda x: -x[1])
        if stale:
            pipelines.append({
                "name": "dead-lanes",
                "status": "ACTION_REQUIRED" if ...
## P2 census-us with backoff

- `15:42:16`   before updated_at=2026-08-25T23:49:53+00:00 phase=COMPLETE
- `15:42:16`   Event invoke accepted (attempt 1) status=202
- `15:51:18`   state still unmoved after 9 min
## P3 what the ceiling means for the lanes

- `15:51:19`   BOJ 60,725/120,394 (50.4%) rows 294,539
- `15:51:19`   census-econ 677/1,226
- `15:51:19`   Both are sharded already. With throttles present the next
- `15:51:19`   lever is NOT more shards -- it is a concurrency limit
- `15:51:19`   increase from AWS Support, or staggering the fan-outs so
- `15:51:19`   they do not all fire on the same minute boundary.
- `15:51:19` ops 5076 GREEN -- ceiling measured before more parallelism
