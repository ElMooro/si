# ops 6698 - live bucket +82.5 GB in a day (alarm justhodl-guard-s3-bytes-growth-1d): who, and is it recurring? (read-only)

**Status:** success  
**Duration:** 339.2s  
**Finished:** 2026-10-27T20:13:14+00:00  

## Log
## A. daily S3 storage metrics, 21 days

- `20:07:35` 2026-10-05  total   1072.2 GB  objects  3,439,799  | GlacierInstantRetrieval 0.5, StandardIA 172.1, Standard 899.6
- `20:07:35` 2026-10-06  total    940.0 GB  objects  3,413,970  | delta  -132.2 GB     -25,829 objects  | GlacierInstantRetrieval 0.6, StandardIA 172.1, Standard 767.3
- `20:07:35` 2026-10-07  total    958.7 GB  objects  3,486,485  | delta   +18.7 GB      72,515 objects  | GlacierInstantRetrieval 0.6, StandardIA 172.1, Standard 786.0
- `20:07:35` 2026-10-08  total    971.6 GB  objects  3,555,804  | delta   +12.9 GB      69,319 objects  | GlacierInstantRetrieval 0.6, StandardIA 172.2, Standard 798.8
- `20:07:35` 2026-10-09  total    992.4 GB  objects  3,648,212  | delta   +20.8 GB      92,408 objects  | GlacierInstantRetrieval 0.7, StandardIA 172.2, Standard 819.5
- `20:07:35` 2026-10-10  total   1003.6 GB  objects  3,686,522  | delta   +11.1 GB      38,310 objects  | GlacierInstantRetrieval 0.7, StandardIA 172.3, Standard 830.6
- `20:07:35` 2026-10-11  total   1031.5 GB  objects  3,747,841  | delta   +27.9 GB      61,319 objects  | GlacierInstantRetrieval 0.8, StandardIA 172.3, Standard 858.4
- `20:07:35` 2026-10-12  total  23133.6 GB  objects  9,113,377  | delta +22102.1 GB   5,365,536 objects  | GlacierInstantRetrieval 0.8, StandardIA 172.3, Standard 22960.5
- `20:07:35` 2026-10-13  total  57122.3 GB  objects 12,105,134  | delta +33988.7 GB   2,991,757 objects  | GlacierInstantRetrieval 0.8, StandardIA 172.3, Standard 56949.2
- `20:07:35` 2026-10-14  total  98774.0 GB  objects 16,351,308  | delta +41651.7 GB   4,246,174 objects  | GlacierInstantRetrieval 0.9, StandardIA 172.3, Standard 98600.8
- `20:07:35` 2026-10-15  total 135575.0 GB  objects 18,469,661  | delta +36800.9 GB   2,118,353 objects  | GlacierInstantRetrieval 0.9, StandardIA 172.4, Standard 135401.6
- `20:07:35` 2026-10-16  total   2114.5 GB  objects  6,795,447  | delta -133460.5 GB  -11,674,214 objects  | GlacierInstantRetrieval 0.9, StandardIA 172.5, Standard 1941.1
- `20:07:35` 2026-10-17  total   1060.5 GB  objects  3,950,632  | delta -1054.0 GB  -2,844,815 objects  | GlacierInstantRetrieval 1.0, StandardIA 172.5, Standard 887.0
- `20:07:35` 2026-10-18  total   1060.7 GB  objects  3,966,082  | delta    +0.2 GB      15,450 objects  | GlacierInstantRetrieval 1.0, StandardIA 172.6, Standard 887.1
- `20:07:35` 2026-10-19  total   1061.3 GB  objects  3,981,806  | delta    +0.6 GB      15,724 objects  | GlacierInstantRetrieval 1.1, StandardIA 172.6, Standard 887.7
- `20:07:35` 2026-10-20  total   1038.0 GB  objects  3,807,457  | delta   -23.3 GB    -174,349 objects  | GlacierInstantRetrieval 1.1, StandardIA 172.6, Standard 864.3
- `20:07:35` 2026-10-21  total   1049.1 GB  objects  3,887,848  | delta   +11.1 GB      80,391 objects  | GlacierInstantRetrieval 1.1, StandardIA 172.6, Standard 875.3
- `20:07:35` 2026-10-22  total   1057.3 GB  objects  3,955,672  | delta    +8.2 GB      67,824 objects  | GlacierInstantRetrieval 1.2, StandardIA 172.6, Standard 883.5
- `20:07:35` 2026-10-23  total   1069.0 GB  objects  4,032,787  | delta   +11.7 GB      77,115 objects  | GlacierInstantRetrieval 1.2, StandardIA 172.7, Standard 895.1
- `20:07:35` 2026-10-24  total   1078.7 GB  objects  4,092,738  | delta    +9.6 GB      59,951 objects  | GlacierInstantRetrieval 1.2, StandardIA 172.7, Standard 904.7
- `20:07:35` 2026-10-25  total   1100.0 GB  objects  4,218,216  | delta   +21.3 GB     125,478 objects  | GlacierInstantRetrieval 1.3, StandardIA 172.8, Standard 925.9
- `20:07:35` 2026-10-26  total   1176.8 GB  objects          0  | delta   +76.9 GB  -4,218,216 objects  | GlacierInstantRetrieval 1.3, StandardIA 172.8, Standard 1002.7
## B. bytes written / removed per day (access logs, sampled and scaled)

- `20:08:32` 2026/10/24: 178126 log objects over 23.8 h (sampled 9000, x19.8): written 7.6 GB (7.6 GB/day pace), writes 58761; removed {}
- `20:09:32` 2026/10/25: 186258 log objects over 23.8 h (sampled 9000, x20.7): written 14.5 GB (14.5 GB/day pace), writes 112479; removed {}
- `20:12:10` 2026/10/26: 406803 log objects over 23.9 h (sampled 9000, x45.2): written 88.8 GB (88.8 GB/day pace), writes 672671; removed {}
- `20:13:11` 2026/10/27: 199050 log objects over 19.0 h (sampled 9000, x22.1): written 79.9 GB (100.8 GB/day pace), writes 244986; removed {}
## bytes written by writer (GB, scaled per log-day; the last day is partial)

- `20:13:11` justhodl-worldbank-full                      10/24    0.0 GB (      0 writes) | 10/25    0.0 GB (      0 writes) | 10/26   70.8 GB (  37290 writes) | 10/27   75.6 GB (  50005 writes)
- `20:13:11` justhodl-src-mirror                          10/24    0.0 GB (      0 writes) | 10/25    0.1 GB (   1034 writes) | 10/26    7.9 GB (   1401 writes) | 10/27    0.1 GB (   1105 writes)
- `20:13:11` justhodl-fmp-bank                            10/24    0.0 GB (     19 writes) | 10/25    4.8 GB (    682 writes) | 10/26    0.0 GB (      0 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-fmp-walker                          10/24    0.0 GB (      0 writes) | 10/25    2.8 GB (  26614 writes) | 10/26    0.0 GB (      0 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-equity-research                     10/24    2.7 GB (  13379 writes) | 10/25    0.7 GB (   3000 writes) | 10/26    0.1 GB (    858 writes) | 10/27    0.1 GB (    575 writes)
- `20:13:11` justhodl-polygon-eod                         10/24    0.0 GB (      0 writes) | 10/25    0.1 GB (  25186 writes) | 10/26    1.6 GB ( 118922 writes) | 10/27    0.3 GB (  12938 writes)
- `20:13:11` justhodl-analytics-snapshot                  10/24    0.0 GB (      0 writes) | 10/25    0.0 GB (      0 writes) | 10/26    1.6 GB (    361 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-polygon-options-flow                10/24    0.0 GB (      0 writes) | 10/25    0.0 GB (      0 writes) | 10/26    1.3 GB (   5288 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-fundamental-graphs                  10/24    1.2 GB (   7679 writes) | 10/25    0.2 GB (   1221 writes) | 10/26    0.0 GB (    497 writes) | 10/27    0.0 GB (    287 writes)
- `20:13:11` justhodl-bis-gleif                           10/24    0.9 GB (    118 writes) | 10/25    0.0 GB (      0 writes) | 10/26    0.0 GB (      0 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-provider-catalog                    10/24    0.0 GB (      0 writes) | 10/25    0.9 GB (   4594 writes) | 10/26    0.6 GB (   9040 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-bank-histories                      10/24    0.0 GB (      0 writes) | 10/25    0.0 GB (      0 writes) | 10/26    0.9 GB ( 351794 writes) | 10/27    0.4 GB ( 118965 writes)
- `20:13:11` runner:github-actions-justhodl               10/24    0.1 GB (  15002 writes) | 10/25    0.1 GB (  18584 writes) | 10/26    0.8 GB (   2802 writes) | 10/27    0.0 GB (   1216 writes)
- `20:13:11` justhodl-options-flow-scanner                10/24    0.0 GB (    989 writes) | 10/25    0.8 GB (   3145 writes) | 10/26    0.0 GB (      0 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-risk-gate                           10/24    0.5 GB (    277 writes) | 10/25    0.1 GB (     41 writes) | 10/26    0.0 GB (     90 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-cboe-options-chain                  10/24    0.0 GB (      0 writes) | 10/25    0.5 GB (     41 writes) | 10/26    0.0 GB (      0 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-xbrl-fundamentals                   10/24    0.0 GB (      0 writes) | 10/25    0.5 GB (   1055 writes) | 10/26    0.0 GB (      0 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-whats-changed                       10/24    0.0 GB (      0 writes) | 10/25    0.5 GB (    310 writes) | 10/26    0.0 GB (      0 writes) | 10/27    0.0 GB (      0 writes)
- `20:13:11` justhodl-tail-risk                           10/24    0.0 GB (    217 writes) | 10/25    0.0 GB (      0 writes) | 10/26    0.4 GB (   4746 writes) | 10/27    0.2 GB (   1946 writes)
- `20:13:11` justhodl-symbology-master                    10/24    0.0 GB (      0 writes) | 10/25    0.0 GB (      0 writes) | 10/26    0.0 GB (      0 writes) | 10/27    0.4 GB (     22 writes)
- `20:13:11` justhodl-ticker-360                          10/24    0.0 GB (      0 writes) | 10/25    0.3 GB (   2131 writes) | 10/26    0.0 GB (     90 writes) | 10/27    0.4 GB (   2211 writes)
- `20:13:11` justhodl-etf-constituents                    10/24    0.4 GB (   6511 writes) | 10/25    0.1 GB (    393 writes) | 10/26    0.1 GB (   2260 writes) | 10/27    0.0 GB (      0 writes)
## bytes written by writer x folder, Oct 26 and Oct 27 (top 20)

- `20:13:11` 10/26    70.80 GB  justhodl-worldbank-full              data/warm/worldbank-full/
- `20:13:11` 10/26     7.95 GB  justhodl-src-mirror                  data/warm/nyfed-research/
- `20:13:11` 10/26     1.65 GB  justhodl-polygon-eod                 data/warm/polygon-full/
- `20:13:11` 10/26     1.42 GB  justhodl-analytics-snapshot          analytics/(files)
- `20:13:11` 10/26     1.09 GB  justhodl-polygon-options-flow        data/option-flow-research/
- `20:13:11` 10/26     0.89 GB  justhodl-bank-histories              data/warm/history-book/
- `20:13:11` 10/26     0.72 GB  runner:github-actions-justhodl       data/series-cache/
- `20:13:11` 10/26     0.43 GB  justhodl-tail-risk                   audit-private/20260909-originals/tail-research/
- `20:13:11` 10/26     0.31 GB  justhodl-provider-catalog            data/providers/gdelt/
- `20:13:11` 10/26     0.27 GB  justhodl-dollar-radar                audit-private/20260909-originals/dollar-research/
- `20:13:11` 10/26     0.26 GB  justhodl-bank-factors                data/warm/factor-book/
- `20:13:11` 10/26     0.25 GB  justhodl-bond-warroom                data/(files)
- `20:13:11` 10/26     0.22 GB  justhodl-eia-bank                    data/warm/eia-bank/
- `20:13:11` 10/26     0.21 GB  justhodl-etf-global-desk             data/etf-desk-research/
- `20:13:11` 10/26     0.20 GB  justhodl-analytics-snapshot          analytics/_state/
- `20:13:11` 10/26     0.18 GB  justhodl-polygon-options-flow        audit-private/20260909-originals/options-research/
- `20:13:11` 10/26     0.16 GB  justhodl-provider-catalog            data/providers/fmp/
- `20:13:11` 10/26     0.14 GB  justhodl-worldbank-wgi               data/warm/worldbank-wgi/
- `20:13:11` 10/26     0.13 GB  justhodl-polygon-bank                data/warm/polygon-bank/
- `20:13:11` 10/26     0.12 GB  justhodl-boj-full                    data/warm/boj-full/
- `20:13:11` 10/27    75.62 GB  justhodl-worldbank-full              data/warm/worldbank-full/
- `20:13:11` 10/27     0.45 GB  justhodl-bank-histories              data/warm/history-book/
- `20:13:11` 10/27     0.44 GB  justhodl-symbology-master            data/symbology/
- `20:13:11` 10/27     0.42 GB  justhodl-ticker-360                  data/research-network/
- `20:13:11` 10/27     0.35 GB  justhodl-polygon-bank                data/warm/polygon-bank/
- `20:13:11` 10/27     0.27 GB  justhodl-polygon-eod                 data/warm/polygon-full/
- `20:13:11` 10/27     0.24 GB  justhodl-euro-fragmentation          data/(files)
- `20:13:11` 10/27     0.18 GB  justhodl-tradingview                 data/(files)
- `20:13:11` 10/27     0.18 GB  justhodl-tradingview                 audit-private/20260909-originals/fred-vault/
- `20:13:11` 10/27     0.18 GB  justhodl-tail-risk                   audit-private/20260909-originals/tail-research/
- `20:13:11` 10/27     0.14 GB  justhodl-crypto-intel                (root files)
- `20:13:11` 10/27     0.13 GB  justhodl-global-expansion            data/warm/sec-dera/
- `20:13:11` 10/27     0.12 GB  justhodl-crisis-composite            data/(files)
- `20:13:11` 10/27     0.08 GB  justhodl-src-mirror                  data/warm/nyfed-research/
- `20:13:11` 10/27     0.08 GB  justhodl-vintage-fred                data/vintage-research/
- `20:13:11` 10/27     0.08 GB  justhodl-usgov-direct                data/raw/
- `20:13:11` 10/27     0.07 GB  justhodl-symdir                      data/warm/fred-scoped/
- `20:13:11` 10/27     0.07 GB  justhodl-bank-factors                data/warm/factor-book/
- `20:13:11` 10/27     0.07 GB  justhodl-risk-regime                 data/risk-regime-research/
- `20:13:11` 10/27     0.06 GB  justhodl-risk-regime                 audit-private/20260909-originals/risk-regime/
## C. lifecycle rules

- `20:13:11` archive-to-glacier-deep-after-90d [Enabled] filter {'Prefix': 'archive/'} | noncurrent None | expiration None | abort-mpu {'DaysAfterInitiation': 7}
- `20:13:11` expire-old-versions-after-30d [Enabled] filter None | noncurrent None | expiration None | abort-mpu {'DaysAfterInitiation': 7}
- `20:13:11` expire-screener-snapshots-30d [Enabled] filter {'Prefix': 'screener/snapshots/'} | noncurrent None | expiration {'Days': 30} | abort-mpu None
- `20:13:11` jh-noncurrent-14d [Enabled] filter None | noncurrent None | expiration None | abort-mpu {'DaysAfterInitiation': 3}
- `20:13:11` justhodl-e9-warm-ia [Enabled] filter {'Prefix': 'data/warm/'} | noncurrent None | expiration None | abort-mpu None
- `20:13:11` justhodl-e9-raw-glacier-ir [Enabled] filter {'Prefix': 'data/raw/'} | noncurrent None | expiration None | abort-mpu None
- `20:13:11` justhodl-e9-attic-ia [Enabled] filter {'Prefix': 'data/attic/'} | noncurrent None | expiration None | abort-mpu None
- `20:13:11` ops5027-purge-dead-versions-providers [Enabled] filter {'Prefix': 'data/providers/'} | noncurrent None | expiration {'ExpiredObjectDeleteMarker': True} | abort-mpu {'DaysAfterInitiation': 1}
- `20:13:11` ops5164-purge-dead-versions-data [Enabled] filter {'Prefix': 'data/'} | noncurrent None | expiration {'ExpiredObjectDeleteMarker': True} | abort-mpu None
- `20:13:11` ops6520-purge-dead-versions-system-events [Enabled] filter {'Prefix': 'system-events/'} | noncurrent None | expiration {'ExpiredObjectDeleteMarker': True} | abort-mpu None
- `20:13:11` ops6541-bound-noncurrent-versions [Enabled] filter {'Prefix': ''} | noncurrent {'NoncurrentDays': 1, 'NewerNoncurrentVersions': 10} | expiration None | abort-mpu None
## D. versions on the biggest writers' files (sampled keys)

- `20:13:12` data/warm/worldbank-full/: 1 keys sampled, versions/key median 1000 max 1000, current 0.00 GB, old copies 2.07 GB, versions written since Oct 26: 1000
- `20:13:12` data/warm/nyfed-research/: 197 keys sampled, versions/key median 8 max 13, current 0.18 GB, old copies 1.24 GB, versions written since Oct 26: 232
- `20:13:12` data/warm/polygon-full/: 151 keys sampled, versions/key median 6 max 13, current 0.00 GB, old copies 0.01 GB, versions written since Oct 26: 851
- `20:13:13` analytics/(files): 9 keys sampled, versions/key median 12 max 12, current 0.05 GB, old copies 0.24 GB, versions written since Oct 26: 24
- `20:13:13` data/warm/history-book/: 938 keys sampled, versions/key median 1 max 48, current 0.01 GB, old copies 0.00 GB, versions written since Oct 26: 1000
- `20:13:13` data/option-flow-research/: 1000 keys sampled, versions/key median 1 max 1, current 0.06 GB, old copies 0.00 GB, versions written since Oct 26: 13
- `20:13:14` data/series-cache/: 935 keys sampled, versions/key median 1 max 6, current 0.04 GB, old copies 0.05 GB, versions written since Oct 26: 14
- `20:13:14` ✅ ops 6698 complete (read-only)
