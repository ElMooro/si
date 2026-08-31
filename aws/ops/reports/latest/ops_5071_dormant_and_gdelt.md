## P0 orchestrator-driven or actually dead

**Status:** success  
**Duration:** 75.8s  
**Finished:** 2026-08-31T01:26:55+00:00  

## Data

| boj_pct | dead | gdelt_missing | live |
|---|---|---|---|
| 50.4 | 2 | 7381 | 17 |

## Log
- `01:25:39`   candidates from ops 5070: 19
- `01:25:43`   function                                   invokes/7d  state age
- `01:25:43`   justhodl-ecb-deep                               1,017      -1.0h  orchestrator-driven
- `01:25:43`   justhodl-gdelt-full                               298     141.0h  orchestrator-driven
- `01:25:43`   justhodl-velocity-acceleration                    168       0.7h  orchestrator-driven
- `01:25:43`   justhodl-worldbank-full                           134       0.7h  orchestrator-driven
- `01:25:43`   justhodl-imf-full                                  51      22.2h  orchestrator-driven
- `01:25:43`   justhodl-finra-full                                42      22.7h  orchestrator-driven
- `01:25:43`   justhodl-fiscaldata-full                           33      22.7h  orchestrator-driven
- `01:25:43`   justhodl-src-mirror                                16      22.7h  orchestrator-driven
- `01:25:43`   justhodl-dol-full                                  15      22.7h  orchestrator-driven
- `01:25:43`   justhodl-polygon-full                              14      22.7h  orchestrator-driven
- `01:25:43`   justhodl-sec-midas                                 14      22.7h  orchestrator-driven
- `01:25:43`   justhodl-bls-full                                   9      22.7h  orchestrator-driven
- `01:25:43`   justhodl-boe-full                                   8      22.7h  orchestrator-driven
- `01:25:43`   justhodl-frbddp-full                                8      22.7h  orchestrator-driven
- `01:25:43`   justhodl-tic-full                                   8      22.7h  orchestrator-driven
- `01:25:43`   justhodl-asia-trade-full                            6       9.2h  orchestrator-driven
- `01:25:43`   justhodl-hist-banker                                1     116.5h  orchestrator-driven
- `01:25:43`   justhodl-repo                                       0     387.7h  *** DEAD ***
- `01:25:43`   justhodl-fundamental-census                         0     171.7h  *** DEAD ***
- `01:25:43`   running without a rule: 17   genuinely dead: 2
## P1 wire only the dead

- `01:25:45`   rules with free slots: 72
- `01:25:45`   justhodl-repo                      -> benzinga-news-agent-warm (rate(5 minutes))
- `01:25:45`   justhodl-fundamental-census        -> fleet-freshness-monitor-30min (rate(30 minutes))
## P2 reconstruct GDELT's missing slots

- `01:25:46`   state says: files=396882 gaps=7381 cursor=20260831004500
- `01:25:46`   gaps is a COUNTER; only a capped sample survives: ['20150219070000', '20150219073000', '20150219074500', '20150219080000']
- `01:26:52`   listed 401,868 keys, 396,882 distinct 14-digit stamps
- `01:26:52`   stamp range 20150218230000 .. 20260831003000
- `01:26:54`   expected 15-min slots in range: 404,263
- `01:26:54`   present: 396,882   MISSING: 7,381  (state's tally: 7381)
- `01:26:54`   missing by year: {'2015': 204, '2016': 45, '2017': 993, '2018': 990, '2019': 11, '2020': 2585, '2021': 610, '2022': 87, '2023': 172, '2024': 2, '2025': 1681, '2026': 1}
- `01:26:54`   first 6 missing: ['20150219070000', '20150219073000', '20150219074500', '20150219080000', '20150219081500', '20150219083000']
- `01:26:54`   -> data/_state/gdelt-missing-slots.json  (the list the engine never kept; a backfill pass can consume it directly)
## P3 BOJ

- `01:26:55`   boj 60,725/120,394 series (50.4%)  rows 294,539
- `01:26:55` ops 5071 GREEN
