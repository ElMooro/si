# ops 3764 — validate + prune the PPI line universe

**Status:** success  
**Duration:** 83.6s  
**Finished:** 2026-07-23T02:22:02+00:00  

## Data

| dead | flaky | kept | live | verdict |
|---|---|---|---|---|
| 10 | 1 | 188 | 187 | PASS |

## Log
- `02:20:39`   universe in: 198 lines
## A — serial re-test (no concurrency = no false rate-limit)

- `02:20:53`   ... 40/198 checked
- `02:21:06`   ... 80/198 checked
- `02:21:20`   ... 120/198 checked
- `02:21:33`   ... 160/198 checked
- `02:21:42`     DEAD  PCU42993042993022      ERR:HTTP Error 429: Too Many Requests
- `02:21:44`     DEAD  PCU4411104411102       ERR:HTTP Error 429: Too Many Requests
- `02:21:46`     DEAD  PCU441310441310P       ERR:HTTP Error 429: Too Many Requests
- `02:21:48`     DEAD  PCU4413144131          ERR:HTTP Error 429: Too Many Requests
- `02:21:49`     DEAD  PCU44134413            ERR:HTTP Error 429: Too Many Requests
- `02:21:51`     DEAD  PCU441441              ERR:HTTP Error 429: Too Many Requests
- `02:21:53`     DEAD  PCU5182105182104       ERR:HTTP Error 429: Too Many Requests
- `02:21:55`     DEAD  WPU05532101            ERR:HTTP Error 429: Too Many Requests
- `02:21:56`     DEAD  WPU061                 ERR:HTTP Error 429: Too Many Requests
- `02:21:58`     DEAD  WPU06710401            ERR:HTTP Error 429: Too Many Requests
- `02:22:01`     FLAKY WPU067903              recovered on retry
- `02:22:02` ✅   LIVE=187 FLAKY=1 DEAD=10
## B — rewrite the universe (keep LIVE + FLAKY)

- `02:22:02` ✅   wrote config/ppi-lines.json: 188 live lines, 10 pruned
- `02:22:02`     pruned PCU42993042993022      Producer Price Index by Industry: Material Recyclers: Al
- `02:22:02`     pruned PCU4411104411102       Producer Price Index by Industry: New Car Dealers: Servi
- `02:22:02`     pruned PCU441310441310P       Producer Price Index by Industry: Automotive Parts and A
- `02:22:02`     pruned PCU4413144131          Producer Price Index by Industry: Automotive Parts and A
- `02:22:02`     pruned PCU44134413            Producer Price Index by Industry: Automotive Parts, Acce
- `02:22:02`     pruned PCU441441              Producer Price Index by Industry: Motor Vehicle and Part
- `02:22:02`     pruned PCU5182105182104       Producer Price Index by Industry: Data Processing, Hosti
- `02:22:02`     pruned WPU05532101            Producer Price Index by Commodity: Fuels and Related Pro
- `02:22:02`     pruned WPU061                 Producer Price Index by Commodity: Chemicals and Allied 
- `02:22:02`     pruned WPU06710401            Producer Price Index by Commodity: Chemicals and Allied 
## VERDICT

- `02:22:02` ✅ UNIVERSE VALIDATED — re-gate #13 against real coverage
