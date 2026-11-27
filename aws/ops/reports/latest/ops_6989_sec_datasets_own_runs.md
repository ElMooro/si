# ops 6989 - justhodl-sec-datasets: its own runs, read 2026-11-27T01:31+00:00

**Status:** success  
**Duration:** 2.4s  
**Finished:** 2026-11-27T01:31:02+00:00  

## Data

| failed_checks | own_runs | raw_files |
|---|---|---|
| 0 | 0 | 52 |

## Log
## 1. the schedule and the function

- `01:31:00` schedule cron(47 22 * * ? *) ENABLED | function 3008 MB, 900 s, storage 8192 MB, modified 2026-11-27T01:28:27 | self-retries 0
- `01:31:00` ✅ enabled, once a day at 22:47 UTC, a failed run is not retried by itself
## 2. every run on record

- `01:31:00` manifest 32260 bytes, written 2026-11-27T01:11:46+00:00, version 1.0.0 | runs on record: 1
- `01:31:00`   2026-11-27T01:11:45+00:00 | first light (ops)  |   341s | banked  52 files 2.44 GB | months read 13 | quarters read 1 | own memory ? MB | published {"money_funds": "2026-10", "fund_holdings": "2026-06-30"}
- `01:31:00` PENDING: no run of the schedule's own is on record yet (the first is due 2026-11-27 22:47 UTC)
## 3. how far each data set has come

- `01:31:00`   nport     advertised   28 | banked   4 (1.66 GB) | newest 2026q3_nport.zip | read into tables 1 | waiting 3
- `01:31:00`   nmfp      advertised  100 | banked  14 (0.14 GB) | newest 20261008-20261106_nmfp.zip | read into tables 13 | waiting 1
- `01:31:00`   bdc       advertised   23 | banked  11 (0.19 GB) | newest 2026_10_bdc.zip | read into tables 0 | waiting None
- `01:31:00`   form13f   advertised   54 | banked   4 (0.35 GB) | newest 01jun2026-31aug2026_form13f.zip | read into tables 0 | waiting None
- `01:31:00`   insiders  advertised   83 | banked   4 (0.04 GB) | newest 2026q3_form345.zip | read into tables 0 | waiting None
- `01:31:00`   formd     advertised   75 | banked   9 (0.02 GB) | newest 2026q3_d.zip | read into tables 0 | waiting None
- `01:31:00`   ncen      advertised   33 | banked   6 (0.04 GB) | newest 2026q3_ncen.zip | read into tables 0 | waiting None
- `01:31:00` files still to bank: 344 of 396 | 2.44 of about 16.9 GB banked: at 2.44 GB a run the history is complete in about 6 more run(s); one quarter of holdings is read a run (3 waiting)
- `01:31:00` files the engine said it does not read: 0
- `01:31:00` ✅ all seven data sets are in the bank
## 4. raw files against their recorded sizes

- `01:31:01` raw files stored: 52, 2.44 GB | on record: 52
- `01:31:01` ✅ every recorded file is stored at its recorded size
## 5. the published documents

- `01:31:01` data/sec-money-funds.json: as of 2026-10 | 27030 bytes, written 2026-11-27T01:11:45+00:00 | 329 funds, net assets $9061.3bn ($8456.9bn without feeders), repo $2789.4bn, unsecured $626.5bn | history: 2025-10 $2765.8bn, 2025-11 $2834.4bn, 2025-12 $2994.4bn, 2026-01 $2961.0bn, 2026-02 $3003.6bn, 2026-03 $2836.4bn, 2026-04 $2985.7bn, 2026-05 $3003.6bn, 2026-06 $3074.6bn, 2026-07 $2925.3bn, 2026-08 $2832.2bn, 2026-09 $2871.5bn, 2026-10 $2789.4bn
- `01:31:01` net assets without feeder funds, by month (the SEC's own monthly statistics leave feeders out too): 2025-10 $7.930tn (311 funds), 2025-11 $8.055tn (312 funds), 2025-12 $8.180tn (313 funds), 2026-01 $8.217tn (315 funds), 2026-02 $8.341tn (317 funds), 2026-03 $8.271tn (310 funds), 2026-04 $8.188tn (320 funds), 2026-05 $8.387tn (320 funds), 2026-06 $8.442tn (325 funds), 2026-07 $8.387tn (325 funds), 2026-08 $8.439tn (327 funds), 2026-09 $8.384tn (328 funds), 2026-10 $8.457tn (329 funds)
- `01:31:01` ✅ money funds: the newest month and at least thirteen months of history
- `01:31:01` ✅ one row a month, in order
- `01:31:01` data/sec-fund-holdings.json: as of 2026-06-30 from 2026q3_nport.zip | 52920 bytes, written 2026-11-27T01:11:46+00:00 | 13,880 funds, net assets $46075.4bn | quarters read: 2026-06-30 (287,615 securities)
- `01:31:01` ✅ fund holdings: the newest quarter stands
## 6. the Lambda's own report lines

- `01:31:02`   2026-11-27T01:06:03+00:00 | billed 3.4s | memory used 97 MB of 3008
- `01:31:02`   2026-11-27T01:11:45+00:00 | billed 341.2s | memory used 2996 MB of 3008
- `01:31:02` billed in the last three days: 1012 GB-seconds = $0.017
- `01:31:02` ✅ no timeout, crash or out-of-memory line in three days of logs
- `01:31:02` ✅ read complete: nothing was invoked or written
