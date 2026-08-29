## P0 reshape the runtime

**Status:** failure  
**Duration:** 671.9s  
**Finished:** 2026-08-29T14:32:22+00:00  

## Error

```
SystemExit: 1
```

## Log
- `14:21:11`   v3 code present (LastModified=2026-08-29T14:21:05)
- `14:21:15`   runtime: mem=10240 MB timeout=900s status=Successful
- `14:21:17`   reserved concurrency = 1 (serialisation interlock)
- `14:21:18`   cadence rate(5 minutes) -> rate(2 minutes) (ENABLED)
- `14:21:18`   rule err Parameter validation failed:
Missing required parameter in input: "Rule"
Unknown parameter in input: "Name", must be one of: Rule,
## P1 measure

- `14:21:19`   window opens: flows=176 pages=9961 series=4980500
- `14:21:19`   kick sent
- `14:25:00`   t+ 220s flows=232 (+56) pages=18129 (+8168) series=9064500 (+4084000)
- `14:28:41`   t+ 442s flows=271 (+95) pages=35376 (+25415) series=17688000 (+12707500)
- `14:32:22`   t+ 663s flows=331 (+155) pages=52769 (+42808) series=26384500 (+21404000)
## P2 new rate + ETA

- `14:32:22`   BEFORE (ops 5033): 311 pages/min, 155,500 series/min
- `14:32:22`   NOW              : 3874 pages/min, 1937014 series/min  (12.5x)
- `14:32:22`   flows 331 / 8147 (4.06%)  pages 52769 / ~486000 (10.9%)
- `14:32:22`   write errors this run: None   holes recorded: 0
- `14:32:22`   retired failed flows: 0 []
- `14:32:22`   ETA to a complete Eurostat series universe: ~1.9 hours
- `14:32:22`   -> data/ops/eurostat-backfill-progress.json
- `14:32:22` ops 5034 RED: P0:rule
