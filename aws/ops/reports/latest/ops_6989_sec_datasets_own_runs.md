# ops 6989 - justhodl-sec-datasets: its own runs, read 2026-11-27T01:16+00:00

**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-11-27T01:16:05+00:00  

## Data

| failed_checks | own_runs | raw_files |
|---|---|---|
| 0 | 0 | 52 |

## Log
## 1. the schedule and the function

- `01:16:04` schedule cron(47 22 * * ? *) ENABLED | function 3008 MB, 900 s, storage 8192 MB, modified 2026-11-27T01:04:54 | self-retries 0
- `01:16:04` ✅ enabled, once a day at 22:47 UTC, a failed run is not retried by itself
## 2. every run on record

- `01:16:04` manifest 32260 bytes, written 2026-11-27T01:11:46+00:00, version 1.0.0 | runs on record: 1
- `01:16:04`   2026-11-27T01:11:45+00:00 | first light (ops)  |   341s | banked  52 files 2.44 GB | months read 13 | quarters read 1 | published {"money_funds": "2026-10", "fund_holdings": "2026-06-30"}
- `01:16:04` PENDING: no run of the schedule's own is on record yet (the first is due 2026-11-27 22:47 UTC)
## 3. how far each data set has come

- `01:16:04`   nport     advertised   28 | banked   4 (1.66 GB) | newest 2026q3_nport.zip | read into tables 1 | waiting 3
- `01:16:04`   nmfp      advertised  100 | banked  14 (0.14 GB) | newest 20261008-20261106_nmfp.zip | read into tables 13 | waiting 1
- `01:16:04`   bdc       advertised   23 | banked  11 (0.19 GB) | newest 2026_10_bdc.zip | read into tables 0 | waiting None
- `01:16:04`   form13f   advertised   54 | banked   4 (0.35 GB) | newest 01jun2026-31aug2026_form13f.zip | read into tables 0 | waiting None
- `01:16:04`   insiders  advertised   83 | banked   4 (0.04 GB) | newest 2026q3_form345.zip | read into tables 0 | waiting None
- `01:16:04`   formd     advertised   75 | banked   9 (0.02 GB) | newest 2026q3_d.zip | read into tables 0 | waiting None
- `01:16:04`   ncen      advertised   33 | banked   6 (0.04 GB) | newest 2026q3_ncen.zip | read into tables 0 | waiting None
- `01:16:04` files still to bank: 344 of 396 | 2.44 of about 16.9 GB banked: at 2.44 GB a run the history is complete in about 6 more run(s); one quarter of holdings is read a run (3 waiting)
- `01:16:04` files the engine said it does not read: 0
- `01:16:04` ✅ all seven data sets are in the bank
## 4. raw files against their recorded sizes

- `01:16:04` raw files stored: 52, 2.44 GB | on record: 52
- `01:16:04` ✅ every recorded file is stored at its recorded size
## 5. the published documents

- `01:16:05` data/sec-money-funds.json: as of 2026-10 | 27030 bytes, written 2026-11-27T01:11:45+00:00 | 329 funds, net assets $9061.3bn ($8456.9bn without feeders), repo $2789.4bn, unsecured $626.5bn | history: 2025-10 $2765.8bn, 2025-11 $2834.4bn, 2025-12 $2994.4bn, 2026-01 $2961.0bn, 2026-02 $3003.6bn, 2026-03 $2836.4bn, 2026-04 $2985.7bn, 2026-05 $3003.6bn, 2026-06 $3074.6bn, 2026-07 $2925.3bn, 2026-08 $2832.2bn, 2026-09 $2871.5bn, 2026-10 $2789.4bn
- `01:16:05` ✅ money funds: the newest month and at least thirteen months of history
- `01:16:05` ✅ one row a month, in order
- `01:16:05` data/sec-fund-holdings.json: as of 2026-06-30 from 2026q3_nport.zip | 52920 bytes, written 2026-11-27T01:11:46+00:00 | 13,880 funds, net assets $46075.4bn | quarters read: 2026-06-30 (287,615 securities)
- `01:16:05` ✅ fund holdings: the newest quarter stands
## 6. the Lambda's own report lines

- `01:16:05` billed in the last three days: 0 GB-seconds = $0.000
- `01:16:05` ✅ read complete: nothing was invoked or written
