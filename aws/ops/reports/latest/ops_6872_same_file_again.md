# ops 6872 - the same file again: what each engine rewrites without a change (read-only)

**Status:** success  
**Duration:** 93.4s  
**Finished:** 2026-11-21T04:41:50+00:00  

## Data

| day | engines_examined | first_writes | needed_writes | small_engines_writes | unchanged_cost_month | unchanged_rewrites | writes |
|---|---|---|---|---|---|---|---|
| 2026-11-20 | 22 | 20953 | 66742 | 7816 | 38.12 | 127074 | 222585 |

## Log
## A. Who wrote what on 2026-11-20 (access logs, sampled)

- `04:41:40` 411348 log objects, read 9000 (x45.7): about 222584 writes in the day
- `04:41:40` 22 engines wrote at least 600 files; 52 others wrote 7815 between them
## B. The newest two versions of the keys each engine wrote

- `04:41:41` justhodl-bank-histories                        110606 writes | compared  70: same bytes   0, same content   0, stamps only  68, different   2, only version   0 | between writes 24.0 h | avg 7 KB
- `04:41:41`       folders: data/warm/history-book/ 110606
- `04:41:41`       fields that differ (~ = stamp-like): ~as_of x68, lease_until x1, last_run.unchanged x1, last_run.started x1, last_run.notes[length] x1, COLD x1, MIRM x1, PROCF x1
- `04:41:41`       rewritten unchanged, e.g. data/warm/history-book/ratios-bulk/quarter/HLFDY.json.gz, data/warm/history-book/income-statement-growth-bulk/annual/BOBS.json.gz
- `04:41:41` justhodl-bank-factors                           37341 writes | compared  70: same bytes   0, same content   1, stamps only   0, different  69, only version   0 | between writes 10.8 h | avg 4 KB
- `04:41:41`       folders: data/warm/factor-book/ 37341
- `04:41:41`       fields that differ (~ = stamp-like): as_of.polygon-bank x58, as_of.polygon-tape x58, returns.ret_21d x17, returns.ret_252d x14, ranks.spct_volatility_20d x11, ranks.pct_ret_63d x11, ranks.pct_ret_126d x11, ranks.spct_ret_252d x8
- `04:41:41`       rewritten unchanged, e.g. data/warm/factor-book/matrix/cols/cik.json.gz
- `04:41:42` justhodl-polygon-eod                            11426 writes | compared  70: same bytes   0, same content   0, stamps only   0, different  70, only version   0 | between writes 24.0 h | avg 23 KB
- `04:41:42`       folders: data/warm/polygon-full/ 11426
- `04:41:42`       fields that differ (~ = stamp-like): n x70, last x70, rows[length] x70
- `04:41:43` justhodl-worldbank-full                          8546 writes | compared  70: same bytes  52, same content   0, stamps only   0, different  18, only version   0 | between writes 0.0 h | avg 15 KB
- `04:41:43`       folders: data/warm/worldbank-full/ 8546
- `04:41:43`       fields that differ (~ = stamp-like): (not JSON) x18
- `04:41:43`       rewritten unchanged, e.g. data/warm/worldbank-full/src/SABER.TECH.GOAL3.LVL1.zip, data/warm/worldbank-full/src/SE.CLO.15Y.LDGAP.S.PS.2012.1.zip
- `04:41:43` justhodl-polygon-fx-regime                       7449 writes | compared  70: same bytes   0, same content   0, stamps only   0, different  35, only version  35 | between writes 0.0 h | avg 8 KB
- `04:41:43`       folders: audit-private/20260909-originals/fx-research/ 6718, data/fx-quote-research/ 594, data/(files) 137
- `04:41:43`       fields that differ (~ = stamp-like): provider_ticker x27, from x27, exchange_trades_or_volume_observed x27, to x27, notifications_sent x6, engine_invocations x6, provider_requests_this_execution x6, private_account_reads x6
- `04:41:43`       written once, e.g. data/fx-quote-research/runs/9cae794367c3171affc88284a1fb0539c854b14a8c11471db6dba48ff1988b8b.json, audit-private/20260909-originals/fx-research/14d34b3dfbb3e84cac9ea7739fee35172993a67f4aedead7cc979817c12399bb.bin
- `04:41:44` runner:github-actions-justhodl                   5256 writes | compared  70: same bytes   4, same content   0, stamps only   0, different  66, only version   0 | between writes 24.0 h | avg 25 KB
- `04:41:44`       folders: data/watchlist-insights/ 5256
- `04:41:44`       fields that differ (~ = stamp-like): asset_class_weight.Equity x15, holdings[][] x14, top_decreases[][] x9, asset_class_weight.Unreported x6, top_increases[][] x6, holdings[length] x5, asset_class_weight.Cash x4, prior_effective[] x2
- `04:41:44`       rewritten unchanged, e.g. data/watchlist-insights/stock/1C.json, data/watchlist-insights/stock/U1.json
- `04:41:45` justhodl-polygon-bank                            5027 writes | compared  70: same bytes   0, same content   0, stamps only  52, different  18, only version   0 | between writes 156.9 h | avg 1 KB
- `04:41:45`       folders: data/warm/polygon-bank/ 5027
- `04:41:45`       fields that differ (~ = stamp-like): ~request_id x52, results.market_cap x13, results.share_class_shares_outstanding x4, results.weighted_shares_outstanding x3, results[].aum x3, results[].processed_date x3, results[].effective_date x3, results[].discount_premium x2
- `04:41:45`       rewritten unchanged, e.g. data/warm/polygon-bank/ticker-events/ASSX.json.gz, data/warm/polygon-bank/ticker-events/ASHS.json.gz
- `04:41:46` justhodl-eba-risk-dashboard                      4570 writes | compared  70: same bytes   0, same content   0, stamps only  70, different   0, only version   0 | between writes 24.0 h | avg 0 KB
- `04:41:46`       folders: data/warm/eba-risk-dashboard/ 4570
- `04:41:46`       fields that differ (~ = stamp-like): ~generated_at x70
- `04:41:46`       rewritten unchanged, e.g. data/warm/eba-risk-dashboard/series/KRI_SVC_13_PT.json.gz, data/warm/eba-risk-dashboard/series/KRI_LIQ_17_SI.json.gz
- `04:41:46` justhodl-polygon-options-flow                    4387 writes | compared  70: same bytes   0, same content   0, stamps only   0, different  12, only version  58 | between writes 0.0 h | avg 256 KB
- `04:41:46`       folders: audit-private/20260909-originals/options-research/ 3107, data/option-flow-research/ 1279
- `04:41:46`       fields that differ (~ = stamp-like): exchange_chain_completeness_verified x12, capture_is_atomic x12, stop x12, pagination_complete x12
- `04:41:46`       written once, e.g. data/option-flow-research/rows/068860b0de1765200a7bb63e455e9d483e44fe6d4c2a8d8cfa286bb330184133.json, audit-private/20260909-originals/options-research/cfd15358500692c9e80737e4e56593dba7dd8a5894c28e9085b7e671d2e3b65e.bin
- `04:41:46` justhodl-etf-constituents                        2285 writes | compared  50: same bytes   0, same content   0, stamps only   0, different   0, only version  50 | between writes - h | avg 26 KB
- `04:41:46`       folders: audit-private/20260909-originals/etf-constituent-research/ 2285
- `04:41:46`       written once, e.g. audit-private/20260909-originals/etf-constituent-research/e7a0b753a5e754078877d7395d8fdda0c874911fb46b4e83a3b1f9976d400024.bin, audit-private/20260909-originals/etf-constituent-research/000e05876f8563dac211f4ab28ff87270e916a9f1892e5bb2862a921f09be846.bin
- `04:41:46` justhodl-repo                                    2285 writes | compared  50: same bytes  50, same content   0, stamps only   0, different   0, only version   0 | between writes 24.0 h | avg 2 KB
- `04:41:46`       folders: data/repo-history/ 2285
- `04:41:46`       rewritten unchanged, e.g. data/repo-history/BOGZ1FL732051083Q.json, data/repo-history/BOGZ1FL752051003A.json
- `04:41:47` justhodl-momentum-leaders                        2239 writes | compared  49: same bytes   0, same content   0, stamps only   0, different   0, only version  49 | between writes - h | avg 45 KB
- `04:41:47`       folders: data/momentum-leaders/ 2239
- `04:41:47`       written once, e.g. data/momentum-leaders/sources/7fb40931ac7ab335f53537e6b72317acdfe538e06dcd0a17ae36700d44958ffe.json, data/momentum-leaders/sources/35af42a7e02726878cad1aa35ee39a4176836916b172ac6b82c087030ddaf01e.json
- `04:41:47` justhodl-tradingview                             1965 writes | compared  43: same bytes   0, same content   0, stamps only   0, different   0, only version  43 | between writes - h | avg 1 KB
- `04:41:47`       folders: data/evidence/ 1279, data/fred-levels/ 685
- `04:41:47`       written once, e.g. data/evidence/fred/eaf1835a54de4cdd52151d225bf2e205e5fa1142f19e1ccaec24516620e9f5b9/2467dd5dbb58f5ff3f9e23669c101f703250df5f3a18a0e6faaaac55dd305128.bin.gz, data/evidence/fred/b67f617623e8a9ed423ff4ddb5443871c4a4aea83e12346a586397f5e51d8520/367ae656a1cc657251b1ab335afacec631f33a06b06a26e6f92355595387f76c.bin.gz
- `04:41:47` justhodl-credit-stress                           1873 writes | compared  41: same bytes   0, same content   0, stamps only   0, different   1, only version  40 | between writes 0.0 h | avg 46 KB
- `04:41:47`       folders: audit-private/20260909-originals/credit-research/ 1828, data/credit-research/ 45
- `04:41:47`       fields that differ (~ = stamp-like): notifications_sent x1, quality x1, private_account_reads x1, replay x1
- `04:41:47`       written once, e.g. audit-private/20260909-originals/credit-research/ab3a534e8d573891f53152ca7d55bf5c85bbb5d7478634513e8daf42934a0ab5.bin, audit-private/20260909-originals/credit-research/a2dc3b73d1336d367599d5f90cae9a170ba14d407c9351b051ef57c977e02518.bin
- `04:41:47` justhodl-daily-report-v3                         1782 writes | compared  39: same bytes   0, same content   0, stamps only   3, different  19, only version  17 | between writes 23.9 h | avg 5 KB
- `04:41:47`       folders: data/report-research/ 1005, data/evidence/ 776
- `04:41:47`       fields that differ (~ = stamp-like): evidence.observations.key x19, evidence.observations.sha256 x19, ~acquired_at x3, evidence.observations.bytes x3
- `04:41:47`       rewritten unchanged, e.g. data/report-research/cache/LNS13026638.json, data/report-research/cache/BORROW.json
- `04:41:47`       written once, e.g. data/evidence/fred/1405bead89fa8eaac919f912b953e02e664372e56f346fa38b3b554d73d51420/c9bd0a8ac2a02465fb6dfac97b04dc6877a6f8b698747ecafbf69e8f75911a25.bin.gz, data/evidence/fred/8673013c197e92203580c187b379758cbd608c4a99fb0b3e42b11e7e0d26a921/fc2f6c50b8d23ed9c21312578ce94832cee9eb4a21d6c2014048abb4dfd82959.bin.gz
- `04:41:48` justhodl-risk-regime                             1736 writes | compared  38: same bytes   0, same content   0, stamps only   0, different   1, only version  37 | between writes 0.2 h | avg 134 KB
- `04:41:48`       folders: data/risk-regime-research/ 1645, audit-private/20260909-originals/risk-regime/ 45, data/(files) 45
- `04:41:48`       fields that differ (~ = stamp-like): option_cohorts.SPY.contract_rows[].delta x1, option_cohorts.SPY.contract_rows[].iv x1, option_cohorts.HYG.contract_rows[].delta x1, option_cohorts.HYG.contract_rows[].iv x1
- `04:41:48`       written once, e.g. data/risk-regime-research/outputs/9b7a59f355fb506511633e46d4a2640661e9b7ee1a490bc579033102bc288842.json, data/risk-regime-research/sources/35d86420deb153cd0659827374d0515feb5f61bbd2c75430e662c8a0ee0270cd/89a61847d9f416673c22a0f41a523395f477d8d3b6500e1257abdd3e07f5a8ef.json
- `04:41:48` justhodl-tail-risk                               1462 writes | compared  31: same bytes   0, same content   0, stamps only   0, different   4, only version  27 | between writes 0.0 h | avg 91 KB
- `04:41:48`       folders: audit-private/20260909-originals/tail-research/ 1051, data/tail-research/ 411
- `04:41:48`       fields that differ (~ = stamp-like): quality x4, notifications_sent x4, private_account_reads x4, replay x4
- `04:41:48`       written once, e.g. audit-private/20260909-originals/tail-research/2e3c7f11277f2a0feed3f73cb3b0df1cd443f38e8d2e2aae3be3fc0cb078f33f.bin, audit-private/20260909-originals/tail-research/a8a8bb1a41e16d2b7aa86e0921413aebe92209b1870099d8d7f337e1b5a38e7c.bin
- `04:41:49` justhodl-dol-full                                1188 writes | compared  25: same bytes  14, same content   0, stamps only   0, different  11, only version   0 | between writes 24.0 h | avg 1006 KB
- `04:41:49`       folders: data/warm/dol-full/ 1188
- `04:41:49`       fields that differ (~ = stamp-like): (not JSON) x9, lease_until x1, bytes_total x1, have.aw9051.csv.bytes x1, have.aw9051.csv.lm x1, bytes x1
- `04:41:49`       rewritten unchanged, e.g. data/warm/dol-full/src/hbook.csv, data/warm/dol-full/src/aw5159.csv
- `04:41:49` justhodl-implied-prob                             914 writes | compared  19: same bytes   0, same content   0, stamps only   0, different   9, only version  10 | between writes 0.0 h | avg 32 KB
- `04:41:49`       folders: data/implied-research/ 822, audit-private/20260909-originals/implied-research/ 91
- `04:41:49`       fields that differ (~ = stamp-like): provider_requests x9, notifications_sent x9, quality x9, private_account_reads x9
- `04:41:49`       written once, e.g. data/implied-research/runs/6be00635142d817c8da8e3d37cd94575c1b7c7d5c6d4476e2d121b7919ef8fcd.json, data/implied-research/inputs/5b41f7d2ac58ee668c5905ea1b2790fdc75e15715f0e29f29256d7624120b373.json
- `04:41:49` justhodl-ciss-stress                              868 writes | compared  19: same bytes   0, same content   0, stamps only   5, different   7, only version   7 | between writes 24.0 h | avg 43 KB
- `04:41:49`       folders: data/ciss-research/ 548, data/evidence/ 319
- `04:41:49`       fields that differ (~ = stamp-like): evidence.bytes x7, evidence.key x7, evidence.sha256 x7, ~acquired_at x5
- `04:41:49`       rewritten unchanged, e.g. data/ciss-research/cache/3bcd2176a941f67eb31897b03de7b405703ff2633f31ef137916a9261c471185.json, data/ciss-research/cache/968931438181a91e825f53814af5d2dc276fb8943570d441b257b07213d0b77e.json
- `04:41:49`       written once, e.g. data/evidence/ecb/8e48520f511775d2b7b88286ab171a8e08c6b16f4a3b0ec6ee0cfa4f72272441/ff28f4bd8552f73af65f4e317aac95cfce6171890b74185c2a6af015c8ada827.bin.gz, data/evidence/ecb/9cb8b935172e8df1c5ab7c347ad5b0e0dfd30f7103c3a0e8ca809a3776f74222/9f302766e6003b059dc04558b9513bd0119a56b97af4e7638abf35df921c1a58.bin.gz
- `04:41:50` justhodl-fdic-bankfind                            822 writes | compared  18: same bytes   1, same content   0, stamps only  17, different   0, only version   0 | between writes 24.0 h | avg 125 KB
- `04:41:50`       folders: data/warm/fdic-bankfind/ 776, data/(files) 45
- `04:41:50`       fields that differ (~ = stamp-like): ~generated_at x16, ~checked_at x1
- `04:41:50`       rewritten unchanged, e.g. data/warm/fdic-bankfind/series/SYS_UNINSURED_SHARE.json.gz, data/warm/fdic-bankfind/series/SYS_NONCURRENT_RATIO.json.gz
- `04:41:50` justhodl-signal-harvester                         731 writes | compared  16: same bytes   0, same content   0, stamps only   0, different   0, only version  16 | between writes - h | avg 2 KB
- `04:41:50`       folders: data/research-forecasts/ 731
- `04:41:50`       written once, e.g. data/research-forecasts/contexts/a24d08d795efbfa05ce068d952965503a350e777920f5e4ff0be23b3c1d58941.json, data/research-forecasts/records/70c946d19fd424df909622a01275310133fd0345ee20fb0d1c9bbc893a279c39.json
## C. Where a write-only-on-change check pays

- `04:41:50` justhodl-bank-histories                        110606 writes a day,  97% unchanged -> about $32.23 a month (only stamps differ)
- `04:41:50` justhodl-worldbank-full                          8546 writes a day,  74% unchanged -> about $1.90 a month (same bytes)
- `04:41:50` justhodl-eba-risk-dashboard                      4570 writes a day, 100% unchanged -> about $1.37 a month (only stamps differ)
- `04:41:50` justhodl-polygon-bank                            5027 writes a day,  74% unchanged -> about $1.12 a month (only stamps differ)
- `04:41:50` justhodl-repo                                    2285 writes a day, 100% unchanged -> about $0.69 a month (same bytes)
- `04:41:50` justhodl-fdic-bankfind                            822 writes a day, 100% unchanged -> about $0.25 a month (only stamps differ)
- `04:41:50` justhodl-dol-full                                1188 writes a day,  56% unchanged -> about $0.20 a month (same bytes)
- `04:41:50` justhodl-bank-factors                           37341 writes a day,   1% unchanged -> about $0.16 a month (other bytes, same content)
- `04:41:50` runner:github-actions-justhodl                   5256 writes a day,   6% unchanged -> about $0.09 a month (same bytes)
- `04:41:50` justhodl-ciss-stress                              868 writes a day,  26% unchanged -> about $0.07 a month (only stamps differ)
- `04:41:50` of about 222584 writes that day: 127074 rewrote a file without changing its data, 66742 changed it, 20953 wrote a key for the first time; 7815 came from engines too small to examine
- `04:41:50` unchanged rewrites cost about $38.12 a month at this rate (both fees); a check in the engines above removes them
- `04:41:50` ✅ read-only; nothing changed
