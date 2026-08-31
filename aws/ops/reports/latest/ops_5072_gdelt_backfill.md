## P0 deploy + the missing list

**Status:** failure  
**Duration:** 2483.0s  
**Finished:** 2026-08-31T02:15:03+00:00  

## Error

```
SystemExit: 1
```

## Log
- `01:33:40`   code fresh 2026-08-31T01:33:33.000+0000 mem=1024 timeout=850
- `01:33:41`   missing list: 7,381 slots (expected 404,263, present 396,882)
- `01:33:41`   by year: {"2015": 204, "2016": 45, "2017": 993, "2018": 990, "2019": 11, "2020": 2585, "2021": 610, "2022": 87, "2023": 172, "2024": 2, "2025": 1681, "2026": 1
- `01:33:41`   engine state: files=396883 gaps=7381 cursor=20260831010000
## P1 fan out and drain

- `01:33:43`   fanout -> b'{"mode": "backfill_fanout", "shards": 12, "invoked": 12}'
- `01:47:03`   t+13min recovered=0 permanent=0 remaining=0 0.00 GB  shards reporting=0
- `01:47:04`   fanout -> b'{"mode": "backfill_fanout", "shards": 12, "invoked": 12}'
- `02:00:25`   t+27min recovered=0 permanent=0 remaining=0 0.00 GB  shards reporting=0
- `02:00:25`   fanout -> b'{"mode": "backfill_fanout", "shards": 12, "invoked": 12}'
- `02:13:46`   t+40min recovered=0 permanent=0 remaining=0 0.00 GB  shards reporting=0
- `02:13:47`   resolved 0 slots in 40 min (0.0/min)
## P2 truth

- `02:13:47`   RECOVERED (fetched and banked): 0
- `02:13:47`   PERMANENT (404 again -- GDELT never published these): 0
- `02:13:47`   remaining to attempt: 0 of 7,381
- `02:13:47`   bytes recovered: 0.00 GB
- `02:13:47`   nothing resolved -- the backfill is not working
- `02:15:01`   v2 export objects in S3 now: 396,885
## P3 the other lanes

- `02:15:02`   boj 60,725/120,394 series (50.4%) rows 294,539
- `02:15:03`   census-econ 382/1226 entries
- `02:15:03`   -> data/ops/gdelt-backfill.json
- `02:15:03` ops 5072 RED: P2:nowork
