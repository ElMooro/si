# ops 6989 - justhodl-sec-datasets: its own runs, read 2026-11-27T23:14+00:00

**Status:** success  
**Duration:** 2.8s  
**Finished:** 2026-11-27T23:14:16+00:00  

## Data

| failed_checks | own_runs | raw_files |
|---|---|---|
| 0 | 1 | 97 |

## Log
## 1. the schedule and the function

- `23:14:14` schedule cron(47 22 * * ? *) ENABLED | function 3008 MB, 900 s, storage 8192 MB, modified 2026-11-27T01:28:27 | self-retries 0
- `23:14:14` ✅ enabled, once a day at 22:47 UTC, a failed run is not retried by itself
## 2. every run on record

- `23:14:14` manifest 59173 bytes, written 2026-11-27T22:51:33+00:00, version 1.0.0 | runs on record: 2
- `23:14:14`   2026-11-27T01:11:45+00:00 | first light (ops)  |   341s | banked  52 files 2.44 GB | months read 13 | quarters read 1 | own memory ? MB | published {"money_funds": "2026-10", "fund_holdings": "2026-06-30"}
- `23:14:14`   2026-11-27T22:51:32+00:00 | the schedule's own |   266s | banked  45 files 2.44 GB | months read 13 | quarters read 1 | own memory 1176 MB | published {"money_funds": "2026-10", "fund_holdings": "2026-06-30"}
- `23:14:14` ✅ the schedule's own last run finished 0.4 hours ago
- `23:14:14` ✅ the schedule's own runs are a day apart (one run so far)
## 3. how far each data set has come

- `23:14:14`   nport     advertised   28 | banked   8 (3.31 GB) | newest 2026q3_nport.zip | read into tables 2 | waiting 6
- `23:14:14`   nmfp      advertised  100 | banked  29 (0.28 GB) | newest 20261008-20261106_nmfp.zip | read into tables 26 | waiting 3
- `23:14:14`   bdc       advertised   23 | banked  16 (0.33 GB) | newest 2026_10_bdc.zip | read into tables 0 | waiting None
- `23:14:14`   form13f   advertised   54 | banked   9 (0.75 GB) | newest 01jun2026-31aug2026_form13f.zip | read into tables 0 | waiting None
- `23:14:14`   insiders  advertised   83 | banked   9 (0.08 GB) | newest 2026q3_form345.zip | read into tables 0 | waiting None
- `23:14:14`   formd     advertised   75 | banked  15 (0.03 GB) | newest 2026q3_d.zip | read into tables 0 | waiting None
- `23:14:14`   ncen      advertised   33 | banked  11 (0.09 GB) | newest 2026q3_ncen.zip | read into tables 0 | waiting None
- `23:14:14` files still to bank: 299 of 396 | 4.88 of about 16.9 GB banked: at 2.44 GB a run the history is complete in about 5 more run(s); one quarter of holdings is read a run (6 waiting)
- `23:14:14` files the engine said it does not read: 0
- `23:14:14` ✅ all seven data sets are in the bank
## 4. raw files against their recorded sizes

- `23:14:14` raw files stored: 97, 4.88 GB | on record: 97
- `23:14:14` ✅ every recorded file is stored at its recorded size
## 5. the published documents

- `23:14:15` data/sec-money-funds.json: as of 2026-10 | 37715 bytes, written 2026-11-27T22:51:33+00:00 | 329 funds, net assets $9061.3bn ($8456.9bn without feeders), repo $2789.4bn, unsecured $626.5bn | history: 2024-09 $2688.4bn, 2024-10 $2433.4bn, 2024-11 $2410.9bn, 2024-12 $2620.2bn, 2025-01 $2550.8bn, 2025-02 $2728.6bn, 2025-03 $2821.3bn, 2025-04 $2851.6bn, 2025-05 $2913.5bn, 2025-06 $3105.3bn, 2025-07 $2977.3bn, 2025-08 $2743.4bn, 2025-09 $2772.4bn, 2025-10 $2765.8bn, 2025-11 $2834.4bn, 2025-12 $2994.4bn, 2026-01 $2961.0bn, 2026-02 $3003.6bn, 2026-03 $2836.4bn, 2026-04 $2985.7bn, 2026-05 $3003.6bn, 2026-06 $3074.6bn, 2026-07 $2925.3bn, 2026-08 $2832.2bn, 2026-09 $2871.5bn, 2026-10 $2789.4bn
- `23:14:15` net assets without feeder funds, by month (the SEC's own monthly statistics leave feeders out too): 2024-09 $6.834tn (312 funds), 2024-10 $6.927tn (304 funds), 2024-11 $7.125tn (307 funds), 2024-12 $7.238tn (306 funds), 2025-01 $7.511tn (307 funds), 2025-02 $7.388tn (309 funds), 2025-03 $7.391tn (309 funds), 2025-04 $7.374tn (309 funds), 2025-05 $7.468tn (308 funds), 2025-06 $7.473tn (309 funds), 2025-07 $7.533tn (309 funds), 2025-08 $7.671tn (310 funds), 2025-09 $7.777tn (310 funds), 2025-10 $7.930tn (311 funds), 2025-11 $8.055tn (312 funds), 2025-12 $8.180tn (313 funds), 2026-01 $8.217tn (315 funds), 2026-02 $8.341tn (317 funds), 2026-03 $8.271tn (310 funds), 2026-04 $8.188tn (320 funds), 2026-05 $8.387tn (320 funds), 2026-06 $8.442tn (325 funds), 2026-07 $8.387tn (325 funds), 2026-08 $8.439tn (327 funds), 2026-09 $8.384tn (328 funds), 2026-10 $8.457tn (329 funds)
- `23:14:15` ✅ money funds: the newest month and at least thirteen months of history
- `23:14:15` ✅ one row a month, in order
- `23:14:15` data/sec-fund-holdings.json: as of 2026-06-30 from 2026q3_nport.zip | 53175 bytes, written 2026-11-27T22:51:33+00:00 | 13,880 funds, net assets $46075.4bn | quarters read: 2026-03-31 (276,374 securities), 2026-06-30 (287,615 securities)
- `23:14:15` ✅ fund holdings: the newest quarter stands
## 6. the Lambda's own report lines

- `23:14:16`   2026-11-27T01:06:03+00:00 | billed 3.4s | memory used 97 MB of 3008
- `23:14:16`   2026-11-27T01:11:45+00:00 | billed 341.2s | memory used 2996 MB of 3008
- `23:14:16`   2026-11-27T22:51:32+00:00 | billed 266.3s | memory used 2998 MB of 3008
- `23:14:16` billed in the last three days: 1795 GB-seconds = $0.030
- `23:14:16` ✅ no timeout, crash or out-of-memory line in three days of logs
- `23:14:16` ✅ read complete: nothing was invoked or written
