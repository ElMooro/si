# ops 6687 - the first scheduled day after the fixes: REPORT lines since 2026-10-26T23:40, the FINRA book, the census

**Status:** success  
**Duration:** 32.2s  
**Finished:** 2026-10-27T18:07:07+00:00  

## Log
## A. REPORT lines per engine since 23:40 UTC

- `18:06:36` justhodl-analytics-snapshot: 1 runs, 0 errors/timeouts | max memory 279 MB | max duration 17.5 s
- `18:06:36`    10-27 09:00:24 | Duration 17486.66 ms Max Memory Used 279 MB
- `18:06:36` justhodl-earnings-whisper: 1 runs, 0 errors/timeouts | max memory 219 MB | max duration 1.5 s
- `18:06:36`    10-27 08:00:42 | Duration 1544.68 ms Max Memory Used 219 MB
- `18:06:38` justhodl-event-flow-monitor: 1 runs, 0 errors/timeouts | max memory 100 MB | max duration 2.8 s
- `18:06:38`    10-27 05:00:44 | Duration 2840.51 ms Max Memory Used 100 MB
- `18:06:39` justhodl-signal-genealogy: 2 runs, 0 errors/timeouts | max memory 616 MB | max duration 372.2 s
- `18:06:39`    10-27 06:46:09 | Duration 326164.74 ms Max Memory Used 612 MB
- `18:06:39`    10-27 13:37:19 | Duration 372156.73 ms Max Memory Used 616 MB
- `18:06:39` treasury-auto-updater: 0 runs, 0 errors/timeouts | max memory None MB | max duration None s
- `18:06:40` justhodl-short-interest: 1 runs, 0 errors/timeouts | max memory 382 MB | max duration 192.3 s
- `18:06:40`    10-27 07:18:50 | Duration 192333.27 ms Max Memory Used 382 MB
- `18:06:41` justhodl-bank-factors: 1 runs, 0 errors/timeouts | max memory 1255 MB | max duration 322.5 s
- `18:06:41`    10-27 02:56:07 | Duration 322514.78 ms Max Memory Used 1255 MB
- `18:07:01` justhodl-equity-research: 368 runs, 0 errors/timeouts | max memory 305 MB | max duration 7.8 s
- `18:07:01`    10-27 18:00:12 | Duration 5993.93 ms Max Memory Used 251 MB
- `18:07:01`    10-27 18:00:13 | Duration 6771.21 ms Max Memory Used 248 MB
- `18:07:02` justhodl-eps-revision-velocity: 1 runs, 0 errors/timeouts | max memory 547 MB | max duration 51.4 s
- `18:07:02`    10-27 11:01:04 | Duration 51406.60 ms Max Memory Used 547 MB
- `18:07:03` justhodl-momentum-leaders: 1 runs, 0 errors/timeouts | max memory 162 MB | max duration 9.9 s
- `18:07:03`    10-27 09:49:27 | Duration 9933.83 ms Max Memory Used 162 MB
- `18:07:04` justhodl-earnings-pead: 3 runs, 0 errors/timeouts | max memory 237 MB | max duration 1.1 s
- `18:07:04`    10-27 08:01:46 | Duration 960.32 ms Max Memory Used 235 MB
- `18:07:04`    10-27 08:03:39 | Duration 1022.78 ms Max Memory Used 237 MB
- `18:07:05` justhodl-estimate-revisions: 6 runs, 0 errors/timeouts | max memory 129 MB | max duration 0.9 s
- `18:07:05`    10-27 13:31:56 | Duration 643.10 ms Max Memory Used 125 MB
- `18:07:05`    10-27 13:33:51 | Duration 627.99 ms Max Memory Used 129 MB
- `18:07:06` justhodl-pump-positioning: 1 runs, 0 errors/timeouts | max memory 133 MB | max duration 6.1 s
- `18:07:06`    10-27 15:10:54 | Duration 6093.09 ms Max Memory Used 133 MB
## B. the factor book (02:50 UTC join on v1.0.6b)

- `18:07:06` manifest: version 1.0.6 updated 2026-10-27T02:50:45.023172+00:00 | coverage.finra_short 19302 (polygon short 3885) | sources.finra_short {"source": "research-head-shards", "settlement_date": "2026-10-15", "generated_at": "2026-10-26T21:01:07.590180+00:00", "shards": 256, "shard_bytes": 38661102, "ambiguous_symbols": 146, "unresolved_occurrences": 0, "tickers": 23678} | as_of.finra-short-interest 2026-10-15
- `18:07:06` AAPL: short_source polygon short_pct_float 0.8289 | polygon SI 120763539.0 | FINRA SI 131591865.0 DTC 3.71 change 8.966552396249% vs-polygon -8.2287% settlement 2026-10-15
- `18:07:06` GME: short_source finra short_pct_float 11.398 | polygon SI None | FINRA SI 48031618.0 DTC 4.0 change 12.555495219177% vs-polygon None% settlement 2026-10-15
- `18:07:07` TSLA: short_source finra short_pct_float 2.4556 | polygon SI None | FINRA SI 69350469.0 DTC 2.22 change -2.228695558317% vs-polygon None% settlement 2026-10-15
- `18:07:07` SOFI: short_source finra short_pct_float 17.3245 | polygon SI None | FINRA SI 219640877.0 DTC 4.75 change 2.207292238752% vs-polygon None% settlement 2026-10-15
- `18:07:07` JPM: short_source finra short_pct_float 0.9672 | polygon SI None | FINRA SI 25598896.0 DTC 3.31 change 6.129464791731% vs-polygon None% settlement 2026-10-15
## C. the fleet error census for the same window (ops 6664's code)

- `18:07:07` ✅ sections A-B complete; the census writes its own report (ops_6664_fleet_error_census.md)
