## P0 current truth

**Status:** failure  
**Duration:** 662.7s  
**Finished:** 2026-08-29T13:54:09+00:00  

## Error

```
SystemExit: 1
```

## Log
- `13:43:07`   flows_done=129 / 8147 (1.58%)  n_pages=3961  series=1980500
- `13:43:07`   updated_at=2026-08-29T13:40:14+00:00
- `13:43:07`   errored flows: 1
- `13:43:07`     AVIA_GOEXAC                  MemoryError: 
- `13:43:07`   in-flight flows: 1
- `13:43:07`     AVIA_GOEXAC                  rows_done=0 attempts=6
## P1 enable the schedule

- `13:43:07`   rule justhodl-series-extractor-5min -> ENABLED (rate(5 minutes))
- `13:43:07`   reserved concurrency: unreserved
## P2 observe -- three samples across ~11 minutes

- `13:46:48`   t+ 220s  flows=129 (+0)  pages=3961 (+0)  series=1980500 (+0)
- `13:50:28`   t+ 441s  flows=129 (+0)  pages=3961 (+0)  series=1980500 (+0)
- `13:54:08`   t+ 661s  flows=129 (+0)  pages=3961 (+0)  series=1980500 (+0)
- `13:54:08`   RATE: 0.0 flows/min, 0 pages/min, 0 series/min
- `13:54:08`   NO MOVEMENT across the whole window -- the lane is not converging and needs eyes
## P3 idempotency under the live schedule

- `13:54:08`   page-0000 total_versions=2  written_in_window=0  OK
- `13:54:08`   page-1980 total_versions=2  written_in_window=0  OK
- `13:54:09`   page-3466 total_versions=300  written_in_window=0  OK
- `13:54:09`   page-3500 total_versions=300  written_in_window=0  OK
- `13:54:09`   page-3600 total_versions=300  written_in_window=0  OK
- `13:54:09`   page-3900 total_versions=300  written_in_window=0  OK
- `13:54:09`   (page-3466 held 11,870 versions at ops 5028 -- the ops5027 purge is sweeping them; totals here should keep falling)
## P4 projection

- `13:54:09`   NOTE for the cost inbox: this backfill legitimately writes at a rate close to the anomaly's, because the anomaly was this engine doing the same work and throwing it away. Expect one more Cost Anomaly email covering the import window; it ends when flows_done reaches 8147.
- `13:54:09`   reader justhodl-signal-registry-ingest stays quarantined, so the Object Created events cost nothing; replication stays off, so nothing mirrors to us-west-2
- `13:54:09`   -> data/ops/eurostat-backfill-progress.json
- `13:54:09` ops 5032 RED: P2:stalled
