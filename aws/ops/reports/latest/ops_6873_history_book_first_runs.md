# ops 6873 - history book v1.2.0: the first runs, from its own records

**Status:** success  
**Duration:** 907.3s  
**Finished:** 2026-11-21T05:03:18+00:00  

## Data

| blocks | blocks_signed_off | failed | kicks | left_alone | phase | second_pass_confirmed_unread | second_pass_seconds | second_pass_would_write | version | wall_s | written |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 25 | 21 | 0 | 1 | 210051 | STEADY | 21 | 174.0 | 0 | 1.2.0 | 907 | 109043 |

## Log
## 1. What is deployed

- `04:48:11` justhodl-bank-histories: last updated 2026-11-21T04:47:19.000+0000, code qeNjzDDRoSFktrlw..., 3008 MB, timeout 900 s
- `04:48:11` book before: manifest version 1.1.0, phase BACKFILL, state written 2026-11-21T03:18:28.134199+00:00, last run puts 123623 unchanged 0
- `04:48:14` dry-run invoke of two small blocks: {"ok": true, "dry_run": true, "version": "1.2.0", "blocks": ["price-target-summary-bulk/latest", "peers-bulk/latest"], "would_put": 1401, "unchanged": 26986, "seconds": 1.9, "raw": null}
- `04:48:14` ✅ version 1.2.0 answers, and its dry run left the state file as it was
## 2. The first runs

- `04:48:14` kick 1 -> 202
- `05:00:24` run 1: version 1.2.0, 21 blocks in 721.6 s, phase STEADY | written 109043, left alone 210051 | confirmed without a read: 0 | notes ['budget: balance-sheet-statement-bulk/quarter needs ~111s, skipped this run', 'budget: cash-flow-statement-bulk/quarter not reached', 'budget: key-metrics-bulk/quarter not reached', 'budget: balance-sheet-statement-growth-bulk/quarter not reached']
- `05:00:24`     ratios-bulk/annual                             read in full     docs  14946 | written     13 (changed 0, new 13) | left alone  14933 (old hash 14933) | failed 0 | 16.4 s
- `05:00:24`     ratios-bulk/quarter                            read in full     docs  15250 | written     15 (changed 0, new 15) | left alone  15235 (old hash 15235) | failed 0 | 62.4 s
- `05:00:24`     key-metrics-bulk/annual                        read in full     docs  15363 | written     13 (changed 0, new 13) | left alone  15350 (old hash 15350) | failed 0 | 14.2 s
- `05:00:24`     income-statement-growth-bulk/annual            read in full     docs  15370 | written   2633 (changed 2620, new 13) | left alone  12737 (old hash 12737) | failed 0 | 23.2 s
- `05:00:24`     income-statement-growth-bulk/quarter           read in full     docs  15593 | written  13805 (changed 13768, new 37) | left alone   1788 (old hash 1788) | failed 0 | 108.2 s
- `05:00:24`     balance-sheet-statement-growth-bulk/annual     read in full     docs  15316 | written   2624 (changed 2611, new 13) | left alone  12692 (old hash 12692) | failed 0 | 28.2 s
- `05:00:24`     cash-flow-statement-growth-bulk/annual         read in full     docs  15255 | written     13 (changed 0, new 13) | left alone  15242 (old hash 15242) | failed 0 | 12.2 s
- `05:00:24`     cash-flow-statement-growth-bulk/quarter        read in full     docs  15283 | written     15 (changed 0, new 15) | left alone  15268 (old hash 15268) | failed 0 | 41.6 s
- `05:00:24`     profile-bulk/latest                            read in full     docs  27127 | written  24735 (changed 24652, new 83) | left alone   2392 (old hash 2392) | failed 0 | 100.0 s
- `05:00:24`     ratios-ttm-bulk/latest                         read in full     docs  16756 | written  14048 (changed 14005, new 43) | left alone   2708 (old hash 2708) | failed 0 | 61.0 s
- `05:00:24`     key-metrics-ttm-bulk/latest                    read in full     docs  16755 | written  13996 (changed 13953, new 43) | left alone   2759 (old hash 2759) | failed 0 | 57.6 s
- `05:00:24`     scores-bulk/latest                             read in full     docs  16097 | written  12859 (changed 12819, new 40) | left alone   3238 (old hash 3238) | failed 0 | 49.2 s
- `05:00:24`     rating-bulk/latest                             read in full     docs  13877 | written  13877 (changed 12845, new 1032) | left alone      0 (old hash 0) | failed 0 | 52.6 s
- `05:00:24`     dcf-bulk/latest                                read in full     docs   9290 | written   8957 (changed 8892, new 65) | left alone    333 (old hash 333) | failed 0 | 33.3 s
- `05:00:24`     price-target-summary-bulk/latest               read in full     docs   4251 | written   1383 (changed 1376, new 7) | left alone   2868 (old hash 2868) | failed 0 | 5.7 s
- `05:00:24`     upgrades-downgrades-consensus-bulk/latest      read in full     docs   6912 | written     39 (changed 34, new 5) | left alone   6873 (old hash 6873) | failed 0 | 0.7 s
- `05:00:24`     peers-bulk/latest                              read in full     docs  24136 | written     18 (changed 0, new 18) | left alone  24118 (old hash 24118) | failed 0 | 1.2 s
- `05:00:24`     income-statement-bulk/annual                   read in full     docs  15364 | written      0 (changed 0, new 0) | left alone  15364 (old hash 15364) | failed 0 | 7.2 s
- `05:00:24`     income-statement-bulk/quarter                  read in full     docs  15591 | written      0 (changed 0, new 0) | left alone  15591 (old hash 15591) | failed 0 | 26.6 s
- `05:00:24`     balance-sheet-statement-bulk/annual            read in full     docs  15304 | written      0 (changed 0, new 0) | left alone  15304 (old hash 15304) | failed 0 | 10.1 s
- `05:00:24`     cash-flow-statement-bulk/annual                read in full     docs  15258 | written      0 (changed 0, new 0) | left alone  15258 (old hash 15258) | failed 0 | 8.3 s
- `05:00:24` after 1 kick(s): phase STEADY; 21 of 25 blocks read by 1.2.0 and signed off; pending []
- `05:00:24` documents written in these runs 109043, left alone 210051, failed 0
## 3. A run with nothing new

- `05:03:18` whole-book dry run: 25 blocks looked at, 21 confirmed from their files' signatures without a read, would write 0, 174.0 s
- `05:03:18` ✅ with nothing new in the bank the engine writes nothing: 21 blocks confirmed unread, 0 documents to write, 174.0 s
