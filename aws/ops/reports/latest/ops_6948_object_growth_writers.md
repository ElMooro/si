# ops 6948 - which prefixes of justhodl-dashboard-live were written on Nov 17-25 (the +80-100k object days); read-only census at 2026-11-25T11:19:50.296861+00:00

**Status:** success  
**Duration:** 385.6s  
**Finished:** 2026-11-25T11:26:15+00:00  

## Data

| grown_prefixes | objects_listed | partial | per_day | requests | seconds | top_grown | writers_on_focus_day |
|---|---|---|---|---|---|---|---|
| 3 | 4931138 | False | 11-17 32638, 11-18 32445, 11-19 112119, 11-20 112180, 11-21 282873, 11-22 40930, 11-23 49935, 11-24 64966, 11-25 80904 | 5955 | 385 | audit-private/20260909-originals/financial-statement-research/ 13377; data/options-flow-scanner/sources/ 8567; data/warm/fmp/ 8225 | 16 |

## Log
## A. shards

- `11:20:06` top-level prefixes 81; shards 1171 (deep prefixes split one or two levels); leaf-only listings 17
## B. census

- `11:26:15` listed 4931138 current objects in 5955 requests, 386 s, 1188 of 1188 listings
- `11:26:15` current objects last written per day: 2026-11-17 32638, 2026-11-18 32445, 2026-11-19 112119, 2026-11-20 112180, 2026-11-21 282873, 2026-11-22 40930, 2026-11-23 49935, 2026-11-24 64966, 2026-11-25 80904
- `11:26:15` (the fuse's deltas: Nov 17-20 +42-49k, Nov 21 +99,943, Nov 22 +81,712, Nov 23 +89,738; a day's count here is keys whose LAST write was that day)
## C. writers on 2026-11-23 (prefixes with >= 500 keys last written that day), with their Nov 17-25 shape

- `11:26:15`   data/warm/polygon-bank/                          11-23:  10586 | Nov17-20 6362/5245/5213/7046 | Nov21 7858 Nov22 10460 Nov24 10733 Nov25 0
- `11:26:15`   data/warm/fmp/                                   11-23:   7067 | Nov17-20 0/0/0/485 | Nov21 571 Nov22 587 Nov24 11566 Nov25 0
- `11:26:15`   audit-private/20260909-originals/fx-research/    11-23:   3975 | Nov17-20 3977/3975/3973/3977 | Nov21 41 Nov22 41 Nov24 3957 Nov25 0
- `11:26:15`   data/options-flow-scanner/sources/               11-23:   2821 | Nov17-20 2/2/2/2 | Nov21 3041 Nov22 2705 Nov24 2802 Nov25 0
- `11:26:15`   audit-private/20260909-originals/tail-research/  11-23:   2425 | Nov17-20 2425/2425/2425/2425 | Nov21 25 Nov22 25 Nov24 1843 Nov25 0
- `11:26:15`   data/etf-holdings-research/snapshots/            11-23:   1509 | Nov17-20 1511/1590/1508/1509 | Nov21 1525 Nov22 1525 Nov24 1509 Nov25 0
- `11:26:15`   data/warm/worldbank-wgi/                         11-23:   1287 | Nov17-20 0/0/0/0 | Nov21 0 Nov22 0 Nov24 0 Nov25 0
- `11:26:15`   audit-private/20260909-originals/etf-constituent 11-23:   1265 | Nov17-20 1265/1270/1265/1265 | Nov21 1265 Nov22 1265 Nov24 1265 Nov25 0
- `11:26:15`   data/etf-holdings-research/rows/                 11-23:   1170 | Nov17-20 1172/1347/1168/1170 | Nov21 1202 Nov22 1202 Nov24 1171 Nov25 0
- `11:26:15`   data/etf-holdings-research/comparisons/          11-23:    873 | Nov17-20 874/883/874/874 | Nov21 882 Nov22 882 Nov24 873 Nov25 0
- `11:26:15`   audit-private/20260909-originals/options-researc 11-23:    706 | Nov17-20 698/702/712/727 | Nov21 0 Nov22 0 Nov24 713 Nov25 0
- `11:26:15`   data/warm/fred-scoped/                           11-23:    676 | Nov17-20 958/1206/826/773 | Nov21 297 Nov22 134 Nov24 473 Nov25 451
- `11:26:15`   data/evidence/fred/                              11-23:    661 | Nov17-20 773/547/856/692 | Nov21 811 Nov22 542 Nov24 558 Nov25 18
- `11:26:15`   data/option-flow-research/rows/                  11-23:    653 | Nov17-20 646/650/660/674 | Nov21 0 Nov22 0 Nov24 660 Nov25 0
- `11:26:15`   data/risk-regime-research/sources/               11-23:    594 | Nov17-20 694/691/788/692 | Nov21 12 Nov22 12 Nov24 691 Nov25 0
- `11:26:15`   data/warm/eia-bank/                              11-23:    554 | Nov17-20 482/522/462/503 | Nov21 551 Nov22 514 Nov24 547 Nov25 0
- `11:26:15` 16 prefix(es) wrote >= 500 keys on 2026-11-23
## D. the growth: prefixes whose Nov 21-23 keys exceed 3x their Nov 18-20 keys (and >= 2,000 keys over Nov 21-23)

- `11:26:15`   audit-private/20260909-originals/financial-state Nov21-23  13377 vs Nov18-20     30  (11-17 10, 11-18 9, 11-19 10, 11-20 11, 11-21 7358, 11-22 6017, 11-23 2, 11-24 6054, 11-25 6080)
- `11:26:15`   data/options-flow-scanner/sources/               Nov21-23   8567 vs Nov18-20      6  (11-17 2, 11-18 2, 11-19 2, 11-20 2, 11-21 3041, 11-22 2705, 11-23 2821, 11-24 2802, 11-25 0)
- `11:26:15`   data/warm/fmp/                                   Nov21-23   8225 vs Nov18-20    485  (11-17 0, 11-18 0, 11-19 0, 11-20 485, 11-21 571, 11-22 587, 11-23 7067, 11-24 11566, 11-25 0)
- `11:26:15` ⚠ 3 prefix(es) grew: the top rows are the per-object writers behind the +80-100k days - engine owners are the next read (engine-manifest.json keys/prefixes)
## E. the whole-day picture: top 25 prefixes by keys last written Nov 21-25

- `11:26:15`   data/warm/history-book/                          17:0 18:0 19:76166 20:73767 21:232012 22:980 23:363 24:1218 25:26101
- `11:26:15`   data/warm/polygon-bank/                          17:6362 18:5245 19:5213 20:7046 21:7858 22:10460 23:10586 24:10733 25:0
- `11:26:15`   data/warm/factor-book/                           17:0 18:0 19:0 20:64 21:4 22:0 23:4 24:11 25:27403
- `11:26:15`   audit-private/20260909-originals/financial-state 17:10 18:9 19:10 20:11 21:7358 22:6017 23:2 24:6054 25:6080
- `11:26:15`   data/warm/fmp/                                   17:0 18:0 19:0 20:485 21:571 22:587 23:7067 24:11566 25:0
- `11:26:15`   data/warm/worldbank-full/                        17:548 18:307 19:4873 20:4874 21:12130 22:3447 23:0 24:0 25:2
- `11:26:15`   data/warm/polygon-full/                          17:31 18:28 19:28 20:53 21:99 22:0 23:0 24:335 25:12635
- `11:26:15`   data/options-flow-scanner/sources/               17:2 18:2 19:2 20:2 21:3041 22:2705 23:2821 24:2802 25:0
- `11:26:15`   audit-private/20260909-originals/fx-research/    17:3977 18:3975 19:3973 20:3977 21:41 22:41 23:3975 24:3957 25:0
- `11:26:15`   data/etf-holdings-research/snapshots/            17:1511 18:1590 19:1508 20:1509 21:1525 22:1525 23:1509 24:1509 25:0
- `11:26:15`   audit-private/20260909-originals/etf-constituent 17:1265 18:1270 19:1265 20:1265 21:1265 22:1265 23:1265 24:1265 25:0
- `11:26:15`   data/etf-holdings-research/rows/                 17:1172 18:1347 19:1168 20:1170 21:1202 22:1202 23:1170 24:1171 25:0
- `11:26:15`   audit-private/20260909-originals/tail-research/  17:2425 18:2425 19:2425 20:2425 21:25 22:25 23:2425 24:1843 25:0
- `11:26:15`   data/etf-holdings-research/comparisons/          17:874 18:883 19:874 20:874 21:882 22:882 23:873 24:873 25:0
- `11:26:15`   data/evidence/fred/                              17:773 18:547 19:856 20:692 21:811 22:542 23:661 24:558 25:18
- `11:26:15`   data/warm/eia-bank/                              17:482 18:522 19:462 20:503 21:551 22:514 23:554 24:547 25:0
- `11:26:15`   data/warm/fred-scoped/                           17:958 18:1206 19:826 20:773 21:297 22:134 23:676 24:473 25:451
- `11:26:15`   data/statement-research/records/                 17:0 18:0 19:0 20:0 21:500 22:500 23:0 24:500 25:500
- `11:26:15`   audit-private/20260909-originals/etf-desk-resear 17:474 18:474 19:474 20:474 21:474 22:474 23:474 24:474 25:0
- `11:26:15`   data/evidence/etf_original/                      17:142 18:142 19:143 20:142 21:504 22:421 23:435 24:526 25:0
- `11:26:15`   data/evidence/carry/                             17:94 18:69 19:62 20:62 21:603 22:374 23:301 24:540 25:0
- `11:26:15`   data/repo-history/                               17:0 18:0 19:0 20:0 21:1529 22:81 23:1 24:7 25:185
- `11:26:15`   data/backlog/sources/                            17:379 18:312 19:233 20:241 21:420 22:217 23:388 24:418 25:0
- `11:26:15`   audit-private/20260909-originals/options-researc 17:698 18:702 19:712 20:727 21:0 22:0 23:706 24:713 25:0
- `11:26:15`   audit-private/20260909-originals/provider-fund-f 17:332 18:332 19:332 20:332 21:332 22:332 23:332 24:332 25:0
- `11:26:15` ✅ read-only census; nothing written, held, released or invoked
