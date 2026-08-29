## P0 memory + wait for the v2.2 code

**Status:** success  
**Duration:** 666.4s  
**Finished:** 2026-08-29T14:10:39+00:00  

## Data

| eta_hours | flows | flows_per_min | pages | pct |
|---|---|---|---|---|
| 244.8 | 135 | 0.55 | 7382 | 1.66 |

## Log
- `13:59:32`   before: mem=3008 timeout=280 lastmod=2026-08-29T13:59:31.000+0000
- `13:59:33`   v2.2 code present (LastModified=2026-08-29T13:59:31.000+0000) after 0s
- `13:59:37`   memory now 3008 MB (status Successful)
## P1 clear the stuck in-flight record

- `13:59:37`   flows_done=129 n_pages=3961 series=1980500 errors=1
- `13:59:37`   AVIA_GOEXAC before: {"rows_done": 0, "attempts": 16, "rows_at_last_check": 0}
- `13:59:37`   cleared -- retry counters start clean against the streaming parser
## P2 run and observe

- `13:59:37`   rule justhodl-series-extractor-5min: ENABLED
- `13:59:38`   kick invoke status=202
- `13:59:38`   window opens at flows=129 pages=3961 series=1980500
- `14:03:18`   t+ 220s flows=130 (+1) pages=5161 (+1200) series=2580500 (+600000)
- `14:06:58`   t+ 440s flows=132 (+3) pages=6361 (+2400) series=3180500 (+1200000)
- `14:10:38`   t+ 660s flows=135 (+6) pages=7382 (+3421) series=3691000 (+1710500)
- `14:10:38`   RATE 0.55 flows/min  311 pages/min  155500 series/min
## P3 state after the window

- `14:10:38`   errors=0  retired failed_flows=0
- `14:10:38`   flows 135 / 8147  (1.66%)
- `14:10:38`   ETA to a complete Eurostat series universe: ~244.8 hours (~10.2 days) at the measured rate
- `14:10:39`   -> data/ops/eurostat-backfill-progress.json
- `14:10:39` ops 5033 GREEN -- AVIA_GOEXAC unblocked, lane importing
