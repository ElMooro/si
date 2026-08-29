## P0 baseline

**Status:** failure  
**Duration:** 18.7s  
**Finished:** 2026-08-29T13:37:25+00:00  

## Error

```
SystemExit: 1
```

## Log
- `13:37:07`   BEFORE flows_done=129 n_pages=3961 series=1980500 updated_at=2026-08-29T13:34:58+00:00
- `13:37:07`   run window opens at 2026-08-29T13:37:07+00:00
## P1 one Event invoke

- `13:37:07`   accepted status=202
- `13:37:24`   state advanced after 17s
- `13:37:24`   AFTER  flows_done=129 n_pages=3961 series=1980500
- `13:37:24`   delta: flows +0  pages +0  series +0
## P2 dual proof -- new keys +1, existing keys +0

- `13:37:25`   EXISTING page-3466 total=200  written_this_run=0  OK -- untouched
- `13:37:25`   EXISTING page-3500 total=200  written_this_run=0  OK -- untouched
- `13:37:25`   EXISTING page-3550 total=200  written_this_run=0  OK -- untouched
- `13:37:25`   EXISTING page-3600 total=200  written_this_run=0  OK -- untouched
- `13:37:25`   EXISTING page-0000 total=2  written_this_run=0  OK -- untouched
- `13:37:25`   EXISTING page-1980 total=2  written_this_run=0  OK -- untouched
- `13:37:25`   (page-3466 total was 11,870 versions at ops 5028; the ops5027 lifecycle purge is what is shrinking it)
## P3 re-enable the schedule

- `13:37:25`   NOT enabling -- predicate failed
## P4 projection + purge progress

- `13:37:25`   flows 129 / 8147 (1.58%)
- `13:37:25`   ~1 pages/run -> ~482049 runs -> ~1673.8 days at rate(5 minutes)
- `13:37:25`   in-flight AVIA_GOEXAC rows_done=0 attempts=4
- `13:37:25`   -> data/ops/series-extractor-rearm.json
- `13:37:25` ops 5031 RED: P2:noprogress
