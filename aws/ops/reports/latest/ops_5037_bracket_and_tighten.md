## P0 integrity on the BRACKET predicate

**Status:** success  
**Duration:** 595.4s  
**Finished:** 2026-08-29T16:21:14+00:00  

## Data

| bracket_clean | duty_pct | eta_hours | gb | objects | projected_gb |
|---|---|---|---|---|---|
| True | 70.1 | 10.8 | 146.2 | 524349 | 671 |

## Log
- `16:11:20`   n_pages BEFORE count: 518825  (16:11:20)
- `16:13:29`   objects counted     : 524349  (146.25 GB) over 127s
- `16:13:29`   n_pages AFTER count : 525018  (16:13:28)
- `16:13:29`   bracket 518825 <= 524349 <= 525018 : CLEAN -- every claimed page exists
- `16:13:29`   lane wrote 6193 pages during the count itself
- `16:13:29`   holes=0 failed_flows=0 write_errors_last_run=0
## P1 duty cycle of the serialised worker

- `16:13:29`   last hour: invocations=3  errors=0  throttles=644
- `16:13:29`   execution time=2524s of 3600s  ->  DUTY CYCLE 70.1%
- `16:13:29`   (throttles are expected and harmless -- they are ticks arriving while the single worker is busy, which is the interlock doing its job)
## P2 tighten the cadence

- `16:13:30`   cadence rate(2 minutes) -> rate(1 minute) (ENABLED), targets=1 ['justhodl-series-extractor']
- `16:13:30`   reserved concurrency = 1 (must stay 1 -- it is what makes a tighter cadence safe)
## P3 measured re-projection

- `16:13:30`   measured: 147726 series and 295.5 pages per flow, 272 KB/page
- `16:13:30`   NOW  : 1777/8147 flows (21.81%)  525018 pages  262509000 series  146.2 GB
- `16:13:30`   FULL : ~1.20B series  ~2.4M pages  ~671 GB
- `16:13:30`   (my earlier 241M/126GB came from extrapolating the 79 alphabetical-head flows, which are small -- this projection is off 1,400+ real flows)
- `16:13:30`   storage at $0.023/GB-mo: ~$15/month once complete; one-time PUT at $0.005/1k: ~$12
- `16:17:22`   t+ 231s pages=539405 (+14387) series=269702500 (+7193500)
- `16:21:14`   t+ 463s pages=547166 (+22148) series=273583000 (+11074000)
- `16:21:14`   rate after the cadence change: 2870 pages/min
- `16:21:14`   ETA to the FULL Eurostat series universe: ~10.8 hours
- `16:21:14`   -> data/ops/eurostat-backfill-progress.json
- `16:21:14` ops 5037 GREEN -- data intact, lane tightened, real scale on the record
