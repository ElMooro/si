# ops 6871 - history book v1.2.0, dry run against the live book (read-only)

**Status:** success  
**Duration:** 129.0s  
**Finished:** 2026-11-21T04:39:45+00:00  

## Data

| bank_files | bank_files_written_last_day | bank_files_written_last_week | bank_gb | blocks | blocks_compared | changed_differ | changed_read_back | documents | left_alone | same_identical | same_read_back | seen_through_old_hash | share_written_pct | wall_s | would_write | writes_attempted |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 204 | 37 | 37 | 1.73 | 25 | 25 | 66 | 66 | 381200 | 272157 | 240 | 240 | 272157 | 28.61 | 129 | 109043 | 0 |

## Log
## A. The book as the deployed engine left it

- `04:37:36` deployed version 1.1.0, phase BACKFILL, state written 2026-11-21T03:18:28.134199+00:00; last run: 8 blocks in 775.5s, puts 123623, unchanged 0
- `04:37:36` 25 blocks hold documents, 380856 documents in all; one full pass by the deployed engine writes every one of them
- `04:37:36`   profile-bulk/latest                            symbols  27123 | last pass: written      0 unchanged 27123 failed 0 in     4 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   peers-bulk/latest                              symbols  24182 | last pass: written      0 unchanged 24182 failed 0 in     1 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   ratios-ttm-bulk/latest                         symbols  16808 | last pass: written      0 unchanged 16808 failed 0 in     5 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   key-metrics-ttm-bulk/latest                    symbols  16807 | last pass: written      0 unchanged 16807 failed 0 in     3 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   scores-bulk/latest                             symbols  16107 | last pass: written      0 unchanged 16107 failed 0 in     1 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   income-statement-growth-bulk/quarter           symbols  15615 | last pass: written      0 unchanged 15615 failed 0 in    36 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   balance-sheet-statement-growth-bulk/quarter    symbols  15604 | last pass: written  15604 unchanged 0 failed 0 in   142 s | refreshed 2026-11-21T03:05 | vintage 2026-11-20T12:02
- `04:37:36`   balance-sheet-statement-bulk/quarter           symbols  15603 | last pass: written  15603 unchanged 0 failed 0 in   112 s | refreshed 2026-11-21T03:05 | vintage 2026-11-20T12:02
- `04:37:36`   income-statement-bulk/quarter                  symbols  15591 | last pass: written  15591 unchanged 0 failed 0 in    96 s | refreshed 2026-11-21T03:05 | vintage 2026-11-20T12:02
- `04:37:36`   cash-flow-statement-bulk/quarter               symbols  15458 | last pass: written  15458 unchanged 0 failed 0 in   101 s | refreshed 2026-11-21T03:05 | vintage 2026-11-20T12:02
- `04:37:36`   key-metrics-bulk/quarter                       symbols  15441 | last pass: written  15441 unchanged 0 failed 0 in   116 s | refreshed 2026-11-21T03:05 | vintage 2026-11-20T12:02
- `04:37:36`   income-statement-growth-bulk/annual            symbols  15409 | last pass: written  15409 unchanged 0 failed 0 in    72 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   key-metrics-bulk/annual                        symbols  15403 | last pass: written  15403 unchanged 0 failed 0 in    80 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   income-statement-bulk/annual                   symbols  15364 | last pass: written  15364 unchanged 0 failed 0 in    67 s | refreshed 2026-11-21T03:05 | vintage 2026-11-20T12:02
- `04:37:36`   balance-sheet-statement-growth-bulk/annual     symbols  15355 | last pass: written      0 unchanged 15355 failed 0 in    14 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   cash-flow-statement-growth-bulk/quarter        symbols  15322 | last pass: written  15322 unchanged 0 failed 0 in   111 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   balance-sheet-statement-bulk/annual            symbols  15304 | last pass: written  15304 unchanged 0 failed 0 in    70 s | refreshed 2026-11-21T03:05 | vintage 2026-11-20T12:02
- `04:37:36`   cash-flow-statement-growth-bulk/annual         symbols  15294 | last pass: written      0 unchanged 15294 failed 0 in    10 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   ratios-bulk/quarter                            symbols  15288 | last pass: written  15288 unchanged 0 failed 0 in   138 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   cash-flow-statement-bulk/annual                symbols  15258 | last pass: written  15258 unchanged 0 failed 0 in    70 s | refreshed 2026-11-21T03:05 | vintage 2026-11-20T12:02
- `04:37:36`   ratios-bulk/annual                             symbols  14984 | last pass: written  14984 unchanged 0 failed 0 in    79 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   rating-bulk/latest                             symbols  12918 | last pass: written      0 unchanged 12918 failed 0 in     1 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   dcf-bulk/latest                                symbols   9434 | last pass: written      0 unchanged 9434 failed 0 in     0 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   upgrades-downgrades-consensus-bulk/latest      symbols   6926 | last pass: written      0 unchanged 6926 failed 0 in     0 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
- `04:37:36`   price-target-summary-bulk/latest               symbols   4258 | last pass: written      0 unchanged 4258 failed 0 in     0 s | refreshed 2026-11-20T03:05 | vintage 2026-11-19T10:43
## B. The bank files the book is built from

- `04:37:36` FMP bank manifest updated_at 2026-11-20T12:02:55.733758+00:00 (the value the old hash covered); 196 dated keys
- `04:37:37`   profile-bulk/latest                              4 files    38.7 MB | missing 0 | written by the bank in the last day 4, week 4 | newest 11-20 11:16
- `04:37:37`   ratios-ttm-bulk/latest                           1 files    27.1 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 11:16
- `04:37:37`   key-metrics-ttm-bulk/latest                      1 files    18.3 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 11:16
- `04:37:37`   scores-bulk/latest                               1 files     2.9 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 10:27
- `04:37:37`   rating-bulk/latest                               1 files     0.3 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 10:27
- `04:37:37`   dcf-bulk/latest                                  1 files     0.6 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 10:27
- `04:37:37`   price-target-summary-bulk/latest                 1 files     0.1 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 11:16
- `04:37:37`   upgrades-downgrades-consensus-bulk/latest        1 files     0.1 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 10:27
- `04:37:37`   peers-bulk/latest                                1 files     2.7 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 10:27
- `04:37:37`   income-statement-bulk/annual                     5 files    22.4 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 10:29
- `04:37:37`   income-statement-bulk/quarter                   20 files    82.1 MB | missing 0 | written by the bank in the last day 4, week 4 | newest 11-20 10:33
- `04:37:37`   balance-sheet-statement-bulk/annual              5 files    34.3 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 10:33
- `04:37:37`   balance-sheet-statement-bulk/quarter            20 files   128.7 MB | missing 0 | written by the bank in the last day 4, week 4 | newest 11-20 10:34
- `04:37:37`   cash-flow-statement-bulk/annual                  5 files    23.0 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 10:34
- `04:37:37`   cash-flow-statement-bulk/quarter                20 files    70.6 MB | missing 0 | written by the bank in the last day 4, week 4 | newest 11-20 11:19
- `04:37:37`   ratios-bulk/annual                               4 files    73.5 MB | missing 0 | written by the bank in the last day 0, week 0 | newest 10-31 05:00
- `04:37:37`   ratios-bulk/quarter                             16 files   269.9 MB | missing 0 | written by the bank in the last day 0, week 0 | newest 10-31 05:00
- `04:37:37`   key-metrics-bulk/annual                          5 files    67.8 MB | missing 0 | written by the bank in the last day 0, week 0 | newest 10-31 04:58
- `04:37:37`   key-metrics-bulk/quarter                        17 files   200.4 MB | missing 0 | written by the bank in the last day 0, week 0 | newest 10-31 04:59
- `04:37:37`   income-statement-growth-bulk/annual              5 files    44.0 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 11:19
- `04:37:37`   income-statement-growth-bulk/quarter            20 files   158.3 MB | missing 0 | written by the bank in the last day 4, week 4 | newest 11-20 11:22
- `04:37:37`   balance-sheet-statement-growth-bulk/annual       5 files    64.6 MB | missing 0 | written by the bank in the last day 1, week 1 | newest 11-20 11:22
- `04:37:37`   balance-sheet-statement-growth-bulk/quarter     20 files   225.8 MB | missing 0 | written by the bank in the last day 4, week 4 | newest 11-20 11:23
- `04:37:37`   cash-flow-statement-growth-bulk/annual           5 files    44.8 MB | missing 0 | written by the bank in the last day 0, week 0 | newest 10-31 04:54
- `04:37:37`   cash-flow-statement-growth-bulk/quarter         20 files   125.2 MB | missing 0 | written by the bank in the last day 0, week 0 | newest 10-31 04:58
- `04:37:37` 25 blocks, 204 source files, 1.73 GB; the bank wrote 37 of them in the last day and 37 in the last week
## C. The dry run - v1.2.0 from the checkout, writes refused

- `04:37:41`   key-metrics-ttm-bulk/latest                    docs  16755 | would write  13996 (changed 13953, new 43) | left alone   2759 (seen through the old hash 2759) | orphans 95 |    3 s (read    2 s)
- `04:37:41`       changed, read back 6: 6 differ from the stored object; fields freeCashFlowYieldTTM x6, evToFreeCashFlowTTM x6, evToEBITDATTM x5, marketCap x5, evToSalesTTM x4, evToOperatingCashFlowTTM x4; e.g. FFLO: 1 rows touched, +0 rows [], fields ['currentRatioTTM', 'operatingCycleTTM', 'freeCashFlowYieldTTM', 'interestBurdenTTM', 'returnOnTangibleAssetsTTM'] || SOMLY: 1 rows touched, +0 rows [], fields ['marketCap', 'freeCashFlowYieldTTM', 'interestBurdenTTM', 'evToFreeCashFlowTTM', 'evToEBITDATTM']
- `04:37:42`   profile-bulk/latest                            docs  27127 | would write  24735 (changed 24652, new 83) | left alone   2392 (seen through the old hash 2392) | orphans 79 |    4 s (read    3 s)
- `04:37:42`       changed, read back 6: 6 differ from the stored object; fields change x6, changePercentage x6, marketCap x5, price x5, volume x4, averageVolume x3; e.g. FMXUF: 1 rows touched, +0 rows [], fields ['marketCap', 'price', 'change', 'changePercentage', 'description'] || SMTSX: 1 rows touched, +0 rows [], fields ['marketCap', 'lastDividend', 'price', 'change', 'changePercentage']
- `04:37:42`   scores-bulk/latest                             docs  16097 | would write  12859 (changed 12819, new 40) | left alone   3238 (seen through the old hash 3238) | orphans 50 |    1 s (read    0 s)
- `04:37:42`       changed, read back 6: 6 differ from the stored object; fields altmanZScore x6, marketCap x6; e.g. FGL: 1 rows touched, +0 rows [], fields ['altmanZScore', 'marketCap'] || AEGXF: 1 rows touched, +0 rows [], fields ['altmanZScore', 'marketCap']
- `04:37:42`   ratios-ttm-bulk/latest                         docs  16756 | would write  14048 (changed 14005, new 43) | left alone   2708 (seen through the old hash 2708) | orphans 95 |    4 s (read    3 s)
- `04:37:42`       changed, read back 6: 6 differ from the stored object; fields priceToOperatingCashFlowRatioTTM x5, forwardPriceToEarningsGrowthRatioTTM x5, enterpriseValueMultipleTTM x5, enterpriseValueTTM x4, priceToEarningsGrowthRatioTTM x4, priceToBookRatioTTM x4; e.g. DAWIF: 1 rows touched, +0 rows [], fields ['priceToOperatingCashFlowRatioTTM', 'forwardPriceToEarningsGrowthRatioTTM', 'enterpriseValueTTM', 'enterpriseValueMultipleTTM', 'dividendYieldTTM'] || OCLDY: 1 rows touched, +0 rows [], fields ['priceToOperatingCashFlowRatioTTM', 'forwardPriceToEarningsGrowthRatioTTM', 'enterpriseValueTTM', 'debtToMarketCapTTM', 'enterpriseValueMultipleTTM']
- `04:37:43`   rating-bulk/latest                             docs  13877 | would write  13877 (changed 12845, new 1032) | left alone      0 (seen through the old hash 0) | orphans 73 |    0 s (read    0 s)
- `04:37:43`       changed, read back 6: 6 differ from the stored object; fields -; e.g. VRTX: 0 rows touched, +1 rows ['2026-11-19|None|None'], fields [] || JRSH: 0 rows touched, +1 rows ['2026-11-19|None|None'], fields []
- `04:37:44`   dcf-bulk/latest                                docs   9290 | would write   8957 (changed 8892, new 65) | left alone    333 (seen through the old hash 333) | orphans 209 |    0 s (read    0 s)
- `04:37:44`       changed, read back 6: 6 differ from the stored object; fields -; e.g. PRMRF: 0 rows touched, +1 rows ['2026-11-20|None|None'], fields [] || GSHD: 0 rows touched, +1 rows ['2026-11-20|None|None'], fields []
- `04:37:44`   price-target-summary-bulk/latest               docs   4251 | would write   1383 (changed 1376, new 7) | left alone   2868 (seen through the old hash 2868) | orphans 14 |    0 s (read    0 s)
- `04:37:44`       changed, read back 6: 6 differ from the stored object; fields lastMonthCount x4, lastMonthAvgPriceTarget x4, lastYearAvgPriceTarget x3, lastYearCount x3, lastQuarterCount x2, lastQuarterAvgPriceTarget x2; e.g. PENN: 1 rows touched, +0 rows [], fields ['lastMonthCount', 'lastMonthAvgPriceTarget'] || WVE: 1 rows touched, +0 rows [], fields ['lastMonthCount', 'lastMonthAvgPriceTarget']
- `04:37:44`   upgrades-downgrades-consensus-bulk/latest      docs   6912 | would write     39 (changed 34, new 5) | left alone   6873 (seen through the old hash 6873) | orphans 19 |    0 s (read    0 s)
- `04:37:44`       changed, read back 6: 6 differ from the stored object; fields hold x6, buy x3, sell x3; e.g. SPB: 1 rows touched, +0 rows [], fields ['buy', 'hold'] || MDRX: 1 rows touched, +0 rows [], fields ['hold', 'sell']
- `04:37:45`   peers-bulk/latest                              docs  24136 | would write     18 (changed 0, new 18) | left alone  24118 (seen through the old hash 24118) | orphans 64 |    1 s (read    0 s)
- `04:37:50`   income-statement-bulk/annual                   docs  15364 | would write      0 (changed 0, new 0) | left alone  15364 (seen through the old hash 15364) | orphans 0 |    6 s (read    4 s)
- `04:37:53`   balance-sheet-statement-bulk/annual            docs  15304 | would write      0 (changed 0, new 0) | left alone  15304 (seen through the old hash 15304) | orphans 0 |    8 s (read    5 s)
- `04:38:02`   cash-flow-statement-bulk/annual                docs  15258 | would write      0 (changed 0, new 0) | left alone  15258 (seen through the old hash 15258) | orphans 0 |    8 s (read    5 s)
- `04:38:05`   income-statement-bulk/quarter                  docs  15591 | would write      0 (changed 0, new 0) | left alone  15591 (seen through the old hash 15591) | orphans 0 |   21 s (read   15 s)
- `04:38:19`   ratios-bulk/annual                             docs  14946 | would write     13 (changed 0, new 13) | left alone  14933 (seen through the old hash 14933) | orphans 51 |   13 s (read    7 s)
- `04:38:20`   balance-sheet-statement-bulk/quarter           docs  15603 | would write      0 (changed 0, new 0) | left alone  15603 (seen through the old hash 15603) | orphans 0 |   29 s (read   22 s)
- `04:38:28`   cash-flow-statement-bulk/quarter               docs  15458 | would write      0 (changed 0, new 0) | left alone  15458 (seen through the old hash 15458) | orphans 0 |   25 s (read   18 s)
- `04:38:31`   key-metrics-bulk/annual                        docs  15363 | would write     13 (changed 0, new 13) | left alone  15350 (seen through the old hash 15350) | orphans 53 |   10 s (read    6 s)
- `04:38:41`   income-statement-growth-bulk/annual            docs  15370 | would write   2633 (changed 2620, new 13) | left alone  12737 (seen through the old hash 12737) | orphans 52 |   10 s (read    6 s)
- `04:38:41`       changed, read back 6: 6 differ from the stored object; fields growthNetIncomeFromDiscontinuedOperations x6, growthOtherAdjustmentsToNetIncome x1; e.g. CSBR: 1 rows touched, +0 rows [], fields ['growthNetIncomeFromDiscontinuedOperations'] || NCA: 1 rows touched, +0 rows [], fields ['growthNetIncomeFromDiscontinuedOperations']
- `04:39:01`   ratios-bulk/quarter                            docs  15250 | would write     15 (changed 0, new 15) | left alone  15235 (seen through the old hash 15235) | orphans 53 |   40 s (read   26 s)
- `04:39:03`   key-metrics-bulk/quarter                       docs  15441 | would write      0 (changed 0, new 0) | left alone  15441 (seen through the old hash 15441) | orphans 0 |   35 s (read   21 s)
- `04:39:15`   income-statement-growth-bulk/quarter           docs  15593 | would write  13805 (changed 13768, new 37) | left alone   1788 (seen through the old hash 1788) | orphans 59 |   33 s (read   22 s)
- `04:39:15`       changed, read back 6: 6 differ from the stored object; fields growthNetIncomeFromDiscontinuedOperations x6, growthOtherAdjustmentsToNetIncome x1; e.g. HBANL: 3 rows touched, +0 rows [], fields ['growthNetIncomeFromDiscontinuedOperations'] || YIPCF: 1 rows touched, +0 rows [], fields ['growthNetIncomeFromDiscontinuedOperations']
- `04:39:15`   balance-sheet-statement-growth-bulk/annual     docs  15316 | would write   2624 (changed 2611, new 13) | left alone  12692 (seen through the old hash 12692) | orphans 52 |   14 s (read    8 s)
- `04:39:15`       changed, read back 6: 6 differ from the stored object; fields growthCapitalLeaseObligationsNonCurrent x6; e.g. BKLRF: 1 rows touched, +0 rows [], fields ['growthCapitalLeaseObligationsNonCurrent'] || PTN: 1 rows touched, +0 rows [], fields ['growthCapitalLeaseObligationsNonCurrent']
- `04:39:25`   cash-flow-statement-growth-bulk/annual         docs  15255 | would write     13 (changed 0, new 13) | left alone  15242 (seen through the old hash 15242) | orphans 52 |    9 s (read    6 s)
- `04:39:44`   cash-flow-statement-growth-bulk/quarter        docs  15283 | would write     15 (changed 0, new 15) | left alone  15268 (seen through the old hash 15268) | orphans 54 |   28 s (read   18 s)
- `04:39:45`   balance-sheet-statement-growth-bulk/quarter    docs  15604 | would write      0 (changed 0, new 0) | left alone  15604 (seen through the old hash 15604) | orphans 0 |   41 s (read   27 s)
## D. The proof on documents read back from the bucket

- `04:39:45` called unchanged: 240 read back, 240 equal the stored object in everything but as_of
- `04:39:45` called changed:   66 read back, 66 differ from the stored object, 0 have no stored object, 0 are identical to it
- `04:39:45` called new:       101 read back, 47 have no stored object, 54 do
- `04:39:45` ✅ every document the new code would leave alone is the stored object, bar the stamp (240 of 240 read back)
- `04:39:45` ✅ every document the new code would write really differs from what is stored (66 of 66 read back; 0 had no stored object)
- `04:39:45` ⚠ a document with no stored hash exists in the bucket (it would be rewritten once): ['key-metrics-ttm-bulk/latest CAPNR', 'profile-bulk/latest SIMAW', 'scores-bulk/latest OACCU', 'scores-bulk/latest IVDAW', 'ratios-ttm-bulk/latest LAFAR', 'ratios-ttm-bulk/latest HAVAU', 'rating-bulk/latest CCPPF', 'rating-bulk/latest FMOCY', 'rating-bulk/latest HLKHF', 'rating-bulk/latest ACWRF', 'rating-bulk/latest SDCVF', 'rating-bulk/latest TVAHF']
## E. What it comes to

- `04:39:45` blocks compared 25 of 25 (381200 documents); not compared: none
- `04:39:45` the deployed engine writes all 381200 of these on every pass; v1.2.0 would write 109043 (28.6%) and leave 272157 alone (272157 of them recognised through the old hash)
- `04:39:45`   dated blocks (statements, ratios, metrics, growth): 245999 documents, would write 19131
- `04:39:45`   snapshot blocks (profile, TTM, scores, rating, DCF, targets, grades, peers): 135201 documents, would write 89912
- `04:39:45` read time for the compared blocks on this runner: 224 s in all (the engine's run budget is 840 s; blocks whose files did not change are not read at all)
- `04:39:45` ✅ read-only; every write path of the engine was replaced by a function that raises, and none was reached
