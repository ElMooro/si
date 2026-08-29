## P0 did the Aug-28 state hold

**Status:** success  
**Duration:** 96.6s  
**Finished:** 2026-08-29T13:05:25+00:00  

## Data

| sample_lines | share_pct | still_writing | top_writer |
|---|---|---|---|
| 9673 | 89.1 | True | justhodl-series-extractor |

## Log
- `13:03:48`   justhodl-signal-registry-ingest reserved concurrency = 0  HELD
- `13:03:49`   reader invocations/h (last 30h): 28/07=20306 28/08=20227 28/09=20268 28/10=20240 28/11=20294 28/12=2752
- `13:03:49`   replication: absent (4988 held)
- `13:03:49`   access logging: {"TargetBucket": "justhodl-s3-access-logs-857687956942", "TargetPrefix": "live/", "TargetObjectKeyFormat": {"PartitionedPrefix": {"Partition
## P1 current burn (Cost Explorer, Aug-24 -> today)

- `13:03:50`   GetObject@us-east-1            08-24=982k 08-25=1002k 08-26=912k 08-27=828k 08-28=578k
- `13:03:50`   ListBucket@us-east-1           08-24=75k 08-25=114k 08-26=103k 08-27=87k 08-28=87k
- `13:03:50`   ListBucket@us-west-2           08-24=0k 08-25=0k 08-26=0k 08-27=0k 08-28=0k
- `13:03:50`   PutObject@us-east-1            08-24=874k 08-25=629k 08-26=539k 08-27=498k 08-28=498k
- `13:03:50`   PutObject@us-west-2            08-24=2k 08-25=2k 08-26=2k 08-27=2k 08-28=2k
- `13:03:50`   PutObjectForRepl@us-west-2     08-24=876k 08-25=631k 08-26=333k 08-27=0k 08-28=0k
- `13:03:50`   PutObject us-east-1 on last COMPLETE day 2026-08-28: 497866 ($2.49)
## P2 ATTRIBUTION from S3 server access logs

- `13:03:58`   live/857687956942/us-east-1/justhodl-dashboard-live/2026/08/29/ -> 41000 log objects
- `13:04:04`   live/857687956942/us-east-1/justhodl-dashboard-live/2026/08/28/ -> 29065 log objects
- `13:04:05`   log objects in window: 70065 (678.7 MB)
- `13:04:05`   parsing an evenly spread sample of 700 files (every 100th)
- `13:05:21`   parsed 9673 lines (0 unparsed) from 700 files
- `13:05:21`   --- WRITE operations by requester (sample 5315) ---
- `13:05:21`   justhodl-series-extractor                          4738   89.1%  avg=252093B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-provider-catalog                           400    7.5%  avg=75185B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-backend-agent                               52    1.0%  avg=6967B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-streaming-fanout                            27    0.5%  avg=704B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-sdmx-walker                                 18    0.3%  avg=51714B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-crypto-intel                                12    0.2%  avg=537625B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-page-ai                                     11    0.2%  avg=823B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-equity-research                              9    0.2%  avg=213475B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-ecb-deep                                     4    0.1%  avg=1075505B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-market-tape                                  4    0.1%  avg=709B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-pump-mechanics                               4    0.1%  avg=10338B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-te-fred-mirror                               3    0.1%  avg=8692B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-cusip-map-rebuild                            3    0.1%  avg=375894B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-alpha-confluence                             3    0.1%  avg=25091B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   justhodl-stress-scenarios                             2    0.0%  avg=22444B  ua=['"Boto3/1.42.97 md/Botocore#1.42.97 ua/2.1 os/linux#5.10.255-']
- `13:05:21`   --- WRITE operations by key prefix ---
- `13:05:21`   data/providers/eurostat/                                         4738   89.1%
- `13:05:21`   data/providers/gdelt/                                             300    5.6%
- `13:05:21`   data/providers/fred/                                              100    1.9%
- `13:05:21`   data/a2a/nudge-ledger.json                                         43    0.8%
- `13:05:21`   data/warm/sdmx-walker-summary.json                                  6    0.1%
- `13:05:21`   data/crypto-intel-history.json                                      4    0.1%
- `13:05:21`   crypto-intel.json                                                   4    0.1%
- `13:05:21`   data/_state/sdmx-walk-eurostat.json                                 4    0.1%
- `13:05:21`   data/_state/sdmx-walk-bis.json                                      4    0.1%
- `13:05:21`   data/market-tape.json                                               4    0.1%
- `13:05:21`   data/warm/ecb/                                                      3    0.1%
- `13:05:21`   lake/crypto-intel/dt%253D2026-08-28/                                3    0.1%
- `13:05:21`   data/streaming-config.json                                          3    0.1%
- `13:05:21`   data/_streaming/dollar_radar_last.json                              3    0.1%
- `13:05:21`   data/_streaming/crisis_composite_last.json                          3    0.1%
- `13:05:21`   --- writer x prefix (top pairs) ---
- `13:05:21`   justhodl-series-extractor          -> data/providers/eurostat/                    4738
- `13:05:21`   justhodl-provider-catalog          -> data/providers/gdelt/                        300
- `13:05:21`   justhodl-provider-catalog          -> data/providers/fred/                         100
- `13:05:21`   justhodl-backend-agent             -> data/a2a/nudge-ledger.json                    43
- `13:05:21`   justhodl-sdmx-walker               -> data/warm/sdmx-walker-summary.json             6
- `13:05:21`   justhodl-crypto-intel              -> data/crypto-intel-history.json                 4
- `13:05:21`   justhodl-crypto-intel              -> crypto-intel.json                              4
- `13:05:21`   justhodl-sdmx-walker               -> data/_state/sdmx-walk-eurostat.json            4
- `13:05:21`   justhodl-sdmx-walker               -> data/_state/sdmx-walk-bis.json                 4
- `13:05:21`   justhodl-market-tape               -> data/market-tape.json                          4
- `13:05:21`   justhodl-ecb-deep                  -> data/warm/ecb/                                 3
- `13:05:21`   justhodl-crypto-intel              -> lake/crypto-intel/dt%253D2026-08-28/           3
- `13:05:21`   --- READ by requester (top) ---
- `13:05:21`   justhodl-a2a-bus                                    845
- `13:05:21`   justhodl-risk-gate                                  834
- `13:05:21`   justhodl-series-extractor                           520
- `13:05:21`   (anonymous)                                         399
- `13:05:21`   justhodl-backend-agent                              114
- `13:05:21`   justhodl-crypto-intel                               113
- `13:05:21`   justhodl-feed-catalog                               101
- `13:05:21`   justhodl-regime-anomaly                             100
- `13:05:21`   --- LIST by requester (top) ---
- `13:05:21`   justhodl-provider-catalog                           400
- `13:05:21`   justhodl-data-census                                100
- `13:05:21`   justhodl-fleet-monitor                              100
- `13:05:21`   justhodl-signal-harvester                           100
- `13:05:21`   justhodl-series-extractor                            90
- `13:05:21`   justhodl-a2a-bus                                      8
- `13:05:21`   justhodl-import-sentinel                              2
- `13:05:21`   justhodl-carry-surface                                1
- `13:05:21`   --- extrapolated to 497866 real PutObject/day ---
- `13:05:21`   justhodl-series-extractor                      ~   443817 writes/day
- `13:05:21`   justhodl-provider-catalog                      ~    37469 writes/day
- `13:05:21`   justhodl-backend-agent                         ~     4871 writes/day
- `13:05:21`   justhodl-streaming-fanout                      ~     2529 writes/day
- `13:05:21`   justhodl-sdmx-walker                           ~     1686 writes/day
- `13:05:21`   justhodl-crypto-intel                          ~     1124 writes/day
## P3 S3 Inventory -- where the 9.7M objects actually live

- `13:05:21`   newest manifest: inventory/justhodl-dashboard-live/daily-current/2026-08-28T01-00Z/manifest.json (2026-08-28 16:42:25+00:00)
- `13:05:21`   inventory parts: 4  schema: ['Bucket', 'Key', 'Size', 'LastModifiedDate', 'StorageClass', 'IsMultipartUploaded']
- `13:05:24`   read 600000 inventory rows; top prefixes by OBJECT COUNT:
- `13:05:24`   data/warm/fred-scoped/                                          277453      10.7 GB
- `13:05:24`   data/warm/gdelt-full/                                           236010      79.2 GB
- `13:05:24`   data/warm/eurostat/                                               8148       8.9 GB
- `13:05:24`   data/warm/boj-full/                                               7192       0.0 GB
- `13:05:24`   data/archive/convergence-radar/                                   4201       1.7 GB
- `13:05:24`   data/providers/eurostat/                                          3979       1.0 GB
- `13:05:24`   archive/repo/2026/                                                2789       0.1 GB
- `13:05:24`   data/warm/fred-catalog/                                           2229       0.4 GB
- `13:05:24`   data/archive/pump-mechanics/                                      2098       0.0 GB
- `13:05:24`   data/archive/pump-positioning/                                    2097       0.1 GB
- `13:05:24`   data/archive/pair-trades/                                         2095       0.1 GB
- `13:05:24`   data/archive/momentum-leaders/                                    2093       0.2 GB
- `13:05:24`   data/archive/velocity-acceleration/                               1987       0.0 GB
- `13:05:24`   data/raw/nyfed/                                                   1791       0.0 GB
- `13:05:24`   data/warm/bls-full/                                               1661      43.8 GB
- `13:05:24`   archive/intelligence/2026/                                        1579       0.0 GB
- `13:05:24`   data/warm/blackswan/                                              1301       0.0 GB
- `13:05:24`   data/fundgraph/cache/                                             1125       1.2 GB
- `13:05:24`   data/providers/gdelt/                                              803       0.1 GB
- `13:05:24`   data/archive/ai-website-synthesis/                                 639       0.0 GB
## P4 growth + verdict

- `13:05:24`   objects: 08-17=4.84M 08-18=5.23M 08-19=5.72M 08-20=6.22M 08-21=6.72M 08-22=7.13M 08-23=7.74M 08-24=8.63M 08-25=9.14M 08-26=9.69M 08-27=10.17M
- `13:05:24`   last-day delta: +474788 objects
- `13:05:24`   WRITER NAMED: justhodl-series-extractor  (89.1% of all writes) into ['data/providers/eurostat/']
- `13:05:25`   evidence -> data/ops/s3-writer-attribution.json
- `13:05:25` ops 5026 GREEN -- writer named from access logs, not inferred
