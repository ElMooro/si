## P0 Cost Explorer -- S3 daily by USAGE_TYPE (Aug-1 -> today)

**Status:** success  
**Duration:** 27.9s  
**Finished:** 2026-08-28T12:04:39+00:00  

## Data

| inventory | logging | still_burning | tier1_base |
|---|---|---|---|
| ON | ON | True | 2.87 |

## Log
- `12:04:12`   days covered: 2026-08-01 .. 2026-08-28 (28)
- `12:04:12`   USW2-Requests-SIA-Tier1                    $   94.81 total
- `12:04:12`   Requests-Tier1                             $   55.70 total
- `12:04:12`   USE1-USW2-AWS-Out-Bytes                    $   45.99 total
- `12:04:12`   TimedStorage-ByteHrs                       $   18.21 total
- `12:04:12`   DataTransfer-Out-Bytes                     $   17.30 total
- `12:04:12`   USW2-TimedStorage-SIA-ByteHrs              $    9.69 total
- `12:04:12`   Requests-Tier2                             $    8.95 total
- `12:04:12`   USW2-TimedStorage-SIA-SmObjects            $    1.47 total
- `12:04:12`   USW2-Requests-Tier1                        $    0.56 total
- `12:04:12`   USW2-TimedStorage-ByteHrs                  $    0.11 total
- `12:04:12`   USW2-Retrieval-SIA                         $    0.07 total
- `12:04:12`   USW2-DataTransfer-Out-Bytes                $    0.01 total
- `12:04:12`   -- daily $ for the top usage types --
- `12:04:12`   date       ts-SIA-Tier1 quests-Tier1 WS-Out-Bytes rage-ByteHrs er-Out-Bytes -SIA-ByteHrs quests-Tier2 IA-SmObjects
- `12:04:12`   2026-08-01         0.17         0.08         0.37         0.08         0.00         0.03         0.04         0.03
- `12:04:12`   2026-08-02         0.14         0.06         0.33         0.09         0.00         0.04         0.04         0.03
- `12:04:12`   2026-08-03         0.19         0.08         0.33         0.10         0.00         0.04         0.06         0.03
- `12:04:12`   2026-08-04         0.18         0.08         0.20         0.12         0.00         0.05         0.04         0.03
- `12:04:12`   2026-08-05         0.21         0.11         0.02         0.12         0.00         0.05         0.12         0.03
- `12:04:12`   2026-08-06         0.44         0.34         0.08         0.12         0.00         0.05         0.87         0.03
- `12:04:12`   2026-08-07         0.49         0.29         0.88         0.12         0.00         0.06         0.50         0.04
- `12:04:12`   2026-08-08         0.32         0.17         0.60         0.16         0.00         0.07         0.12         0.04
- `12:04:12`   2026-08-09         4.27         2.19         1.93         0.19         0.00         0.08         0.35         0.04
- `12:04:12`   2026-08-10         5.07         2.59         2.13         0.25         0.00         0.13         0.35         0.04
- `12:04:12`   2026-08-11         5.34         2.76         2.16         0.35         0.00         0.17         0.36         0.04
- `12:04:12`   2026-08-12         5.34         2.78         2.14         0.42         0.00         0.21         0.38         0.05
- `12:04:12`   2026-08-13         5.38         2.83         2.13         0.49         0.00         0.26         0.40         0.05
- `12:04:12`   2026-08-14         5.06         2.73         2.09         0.56         0.00         0.31         0.38         0.06
- `12:04:12`   2026-08-15         4.72         2.56         2.11         0.64         0.00         0.35         0.36         0.06
- `12:04:12`   2026-08-16         4.92         2.66         2.09         0.71         0.81         0.39         0.38         0.06
- `12:04:12`   2026-08-17         4.97         2.68         2.09         0.79         2.15         0.43         0.39         0.06
- `12:04:12`   2026-08-18         4.86         2.64         2.12         0.87         1.74         0.47         0.38         0.06
- `12:04:12`   2026-08-19         4.83         2.62         2.39         0.94         1.57         0.51         0.38         0.07
- `12:04:12`   2026-08-20         4.78         2.59         2.14         1.03         1.67         0.56         0.38         0.07
- `12:04:12`   2026-08-21         4.78         2.59         2.18         1.11         1.48         0.60         0.37         0.07
- `12:04:12`   2026-08-22         4.73         2.56         2.14         1.20         1.74         0.65         0.37         0.07
- `12:04:12`   2026-08-23         4.94         2.76         2.59         1.27         1.70         0.69         0.39         0.07
- `12:04:12`   2026-08-24         8.85         4.78         4.33         1.42         1.40         0.76         0.40         0.07
- `12:04:12`   2026-08-25         6.46         3.78         4.43         1.55         1.47         0.83         0.41         0.08
- `12:04:12`   2026-08-26         3.36         3.22         2.01         1.70         1.16         0.93         0.37         0.09
- `12:04:12`   2026-08-27         0.02         2.93         0.00         1.80         0.38         0.96         0.33         0.09
- `12:04:12`   2026-08-28         0.00         0.24         0.00         0.00         0.02         0.00         0.03         0.00
## P1 Cost Explorer -- S3 daily by OPERATION x REGION (14d)

- `12:04:12`   top operations by 14d $ (qty = usage units; requests for request ops, byte-hours/GB-mo for storage):
- `12:04:12`   PutObjectForRepl             us-west-2  $   66.76  qty=       6677110
- `12:04:12`   PutObject                    us-east-1  $   37.00  qty=       7400967
- `12:04:12`   GetObjectForRepl             us-east-1  $   32.71  qty=          1636
- `12:04:12`   GetObject                    us-east-1  $   22.58  qty=      13198528
- `12:04:12`   StandardStorage              us-east-1  $   15.60  qty=           678
- `12:04:12`   StandardIAStorage            us-west-2  $    8.44  qty=           675
- `12:04:12`   ListBucket                   us-east-1  $    4.17  qty=        834363
- `12:04:12`   StandardIASizeOverhead       us-west-2  $    0.98  qty=            78
- `12:04:12`   PutObject                    us-west-2  $    0.24  qty=         23923
- `12:04:12`   UploadPartForRepl            us-west-2  $    0.21  qty=         21131
- `12:04:12`   UploadPart                   us-east-1  $    0.11  qty=         22384
- `12:04:12`   StandardStorage              us-west-2  $    0.06  qty=             2
- `12:04:12`   InitiateMultipartUploadForRe us-west-2  $    0.04  qty=          3939
- `12:04:12`   CompleteMultipartUploadForRe us-west-2  $    0.04  qty=          3939
- `12:04:12`   -- daily REQUEST COUNTS, top request ops --
- `12:04:12`   date         PutObject@st-2   PutObject@st-1   GetObject@st-1   GetObject@st-1   ListBucke@st-1   PutObject@st-2
- `12:04:12`   2026-08-14           504713           503223              105           913635            42039             1669
- `12:04:12`   2026-08-15           470615           468807              105           899495            43937             1673
- `12:04:12`   2026-08-16           490092           488340              105           946235            43377             1673
- `12:04:12`   2026-08-17           494953           493216              105           978801            42480             1683
- `12:04:12`   2026-08-18           484069           482228              106           957398            45535             1705
- `12:04:12`   2026-08-19           479585           477883              120           942251            43973             1707
- `12:04:12`   2026-08-20           476125           474113              107           938749            42360             1717
- `12:04:12`   2026-08-21           475615           474052              109           932456            42388             1717
- `12:04:12`   2026-08-22           470690           468848              107           926332            42442             1715
- `12:04:12`   2026-08-23           490401           488765              129           973848            61304             1717
- `12:04:12`   2026-08-24           876363           874339              216           981926            74674             1727
- `12:04:12`   2026-08-25           630581           628642              221          1001756           113538             1733
- `12:04:12`   2026-08-26           333308           539392              100           911513           102531             1743
- `12:04:12`   2026-08-27                0           498349                0           828064            86637             1743
- `12:04:12`   2026-08-28                0            40770                0            66068             7147                0
## P2 Cost Explorer -- whole account, top services (14d)

- `12:04:12`   Amazon Simple Storage Service              $  189.04 /14d  ($12.60/day)
- `12:04:12`   AWS Lambda                                 $   67.40 /14d  ($4.49/day)
- `12:04:12`   Amazon DynamoDB                            $    9.45 /14d  ($0.63/day)
- `12:04:12`   AmazonCloudWatch                           $    8.00 /14d  ($0.53/day)
- `12:04:12`   CloudWatch Events                          $    6.91 /14d  ($0.46/day)
- `12:04:12`   AWS Secrets Manager                        $    1.63 /14d  ($0.11/day)
- `12:04:12`   Amazon EC2 Container Registry (ECR)        $    0.83 /14d  ($0.06/day)
- `12:04:12`   EC2 - Other                                $    0.57 /14d  ($0.04/day)
- `12:04:12`   ACCOUNT daily total: 08-14=16.7 08-15=14.9 08-16=16.3 08-17=18.6 08-18=19.1 08-19=21.9 08-20=20.6 08-21=20.3 08-22=19.9 08-23=21.9 08-24=30.7 08-25=27.5 08-26=21.8 08-27=13.0 08-28=0.9
## P3 Lambda invocations, top-40 over 72h (GetMetricData)

- `12:04:20`   functions: 871
- `12:04:30`   total invocations/72h: 1588624  (529541/day)
- `12:04:30`   justhodl-signal-registry-ingest                 1557590  (519197/day)
- `12:04:30`   justhodl-sdmx-walker                               5489  (  1830/day)
- `12:04:30`   justhodl-a2a-bus                                   2803  (   934/day)
- `12:04:30`   justhodl-series-extractor                          2592  (   864/day)
- `12:04:30`   justhodl-streaming-fanout                          1440  (   480/day)
- `12:04:30`   justhodl-boj-full                                  1224  (   408/day)
- `12:04:30`   justhodl-market-tape                                892  (   297/day)
- `12:04:30`   benzinga-news-agent                                 866  (   289/day)
- `12:04:30`   justhodl-census-us                                  768  (   256/day)
- `12:04:30`   justhodl-fred-catalog                               576  (   192/day)
- `12:04:30`   justhodl-crypto-intel                               546  (   182/day)
- `12:04:30`   justhodl-ecb-deep                                   438  (   146/day)
- `12:04:30`   justhodl-import-sentinel                            432  (   144/day)
- `12:04:30`   justhodl-equity-research                            408  (   136/day)
- `12:04:30`   justhodl-insider-trades                             388  (   129/day)
- `12:04:30`   enhanced-repo-agent                                 375  (   125/day)
- `12:04:30`   justhodl-backend-agent                              288  (    96/day)
- `12:04:30`   justhodl-dex-scanner                                288  (    96/day)
- `12:04:30`   justhodl-gdelt-sentiment                            288  (    96/day)
- `12:04:30`   justhodl-price-redundancy                           288  (    96/day)
- `12:04:30`   justhodl-vix-curve                                  198  (    66/day)
- `12:04:30`   justhodl-fleet-freshness-monitor                    168  (    56/day)
- `12:04:30`   justhodl-repo-monitor                               162  (    54/day)
- `12:04:30`   justhodl-edgar-insiders                             160  (    53/day)
- `12:04:30`   justhodl-research-critique                          152  (    51/day)
- `12:04:30`   justhodl-options-gamma                              147  (    49/day)
- `12:04:30`   justhodl-crypto-funding                             144  (    48/day)
- `12:04:30`   justhodl-sec-8k                                     144  (    48/day)
- `12:04:30`   justhodl-sec-13f                                    144  (    48/day)
- `12:04:30`   justhodl-prepump-summary                            144  (    48/day)
- `12:04:30`   justhodl-convergence-radar                          144  (    48/day)
- `12:04:30`   justhodl-redflag-alerter                            144  (    48/day)
- `12:04:30`   justhodl-trade-ticket-monitor                       126  (    42/day)
- `12:04:30`   openbb-websocket-broadcast                          124  (    41/day)
- `12:04:30`   justhodl-live-pulse                                 108  (    36/day)
- `12:04:30`   justhodl-alpha-compass                              106  (    35/day)
- `12:04:30`   justhodl-nyfed-markets-full                         106  (    35/day)
- `12:04:30`   justhodl-provider-catalog                            92  (    31/day)
- `12:04:30`   justhodl-event-coordinator                           91  (    30/day)
- `12:04:30`   bond-indices-agent                                   87  (    29/day)
## P4 live-bucket config census + object counts

- `12:04:30`   versioning: Enabled
- `12:04:30`   lifecycle archive-to-glacier-deep-after-90d: {"Filter": {"Prefix": "archive/"}, "Status": "Enabled", "Transitions": [{"Days": 90, "StorageClass": "DEEP_ARCHIVE"}], "AbortIncompleteMultipartUpload": {"DaysA
- `12:04:30`   lifecycle expire-old-versions-after-30d: {"Filter": {}, "Status": "Enabled", "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}
- `12:04:30`   lifecycle expire-screener-snapshots-30d: {"Expiration": {"Days": 30}, "Filter": {"Prefix": "screener/snapshots/"}, "Status": "Enabled"}
- `12:04:30`   lifecycle jh-noncurrent-14d: {"Filter": {}, "Status": "Enabled", "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3}}
- `12:04:30`   lifecycle justhodl-e9-warm-ia: {"Filter": {"Prefix": "data/warm/"}, "Status": "Enabled", "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}]}
- `12:04:30`   lifecycle justhodl-e9-raw-glacier-ir: {"Filter": {"Prefix": "data/raw/"}, "Status": "Enabled", "Transitions": [{"Days": 45, "StorageClass": "GLACIER_IR"}]}
- `12:04:30`   lifecycle justhodl-e9-attic-ia: {"Filter": {"Prefix": "data/attic/"}, "Status": "Enabled", "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}]}
- `12:04:30`   access logging BEFORE: "OFF"
- `12:04:30`   S3->Lambda notifications: 8 [["openbb-websocket-broadcast", ["s3:ObjectCreated:*"], {"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/report.json"}]}}], ["openbb-websocket-broadcast", ["s3:ObjectCreated:*"], {"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/macro-nowcast.json"}]}}], ["openbb-websocket-broadcast", ["s3:ObjectCreated:*"], {"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/compound-sig
- `12:04:30`   S3->SQS/SNS notifications: 0/0
- `12:04:30`   EventBridge notifications: off
- `12:04:30`   inventory configs: 0 
- `12:04:30`   request-metrics configs: 0 
- `12:04:30`   intelligent-tiering configs: 1 ['auto-tier-cold-objects']
- `12:04:30`   analytics configs: 0 
- `12:04:30`   replication: none (4988 held)
- `12:04:31`   justhodl-dashboard-live objects: 338542 -> 9690744 (29 pts)
- `12:04:31`     daily: 08-13=2905k 08-14=3373k 08-15=3819k 08-16=4305k 08-17=4836k 08-18=5231k 08-19=5717k 08-20=6219k 08-21=6720k 08-22=7130k 08-23=7737k 08-24=8629k 08-25=9140k 08-26=9691k
- `12:04:31`     StandardStorage: 76.8GB -> 2591.8GB
- `12:04:32`   justhodl-dashboard-live-dr objects: 688925 -> 10094192 (29 pts)
- `12:04:32`     daily: 08-13=3574k 08-14=4058k 08-15=4528k 08-16=5028k 08-17=5436k 08-18=5952k 08-19=6432k 08-20=6884k 08-21=7423k 08-22=7829k 08-23=8330k 08-24=9272k 08-25=9833k 08-26=10094k
- `12:04:33`     StandardStorage: 0.0GB -> 0.0GB
- `12:04:34`     StandardIAStorage: 45.3GB -> 2544.4GB
- `12:04:35`   justhodl-dr-usw2-857687956942 objects: 111339 -> 152824 (26 pts)
- `12:04:35`     daily: 08-13=131k 08-14=132k 08-15=134k 08-16=136k 08-17=137k 08-18=139k 08-19=141k 08-20=142k 08-21=144k 08-22=146k 08-23=148k 08-24=149k 08-25=151k 08-26=153k
- `12:04:35`     StandardStorage: 5.9GB -> 5.9GB
- `12:04:36`     StandardIAStorage: 0.0GB -> 1.1GB
## P5 arm permanent attribution (access logs + inventory + Storage Lens)

- `12:04:36`   logs bucket CREATED justhodl-s3-access-logs-857687956942
- `12:04:37`   logs bucket policy + lifecycle (30d logs / 60d inv) set
- `12:04:39`   ACCESS LOGGING ON: justhodl-dashboard-live -> s3://justhodl-s3-access-logs-857687956942/live/ (requester = engine session name)
- `12:04:39`   S3 Inventory daily-current ON -> s3://justhodl-s3-access-logs-857687956942/inventory/ (first manifest within 48h)
- `12:04:39`   Storage Lens 'justhodl-lens' (advanced activity metrics, prefix depth 3) ON -- 48h to populate
## P6 verdict

- `12:04:39`   us-east-1 Requests-Tier1: Aug10-25 mean $2.87/day; after: 08-26=$3.22 08-27=$2.93 08-28=$0.24
- `12:04:39`   USW2-DataTransfer-In-Bytes after Aug-26: 08-26=$0.00 08-27=$0.00 08-28=$0.00
- `12:04:39`   USW2-DataTransfer-Out-Bytes after Aug-26: 08-26=$0.01 08-27=$0.00 08-28=$0.00
- `12:04:39`   USW2-Requests-SIA-Tier1 after Aug-26: 08-26=$3.36 08-27=$0.02 08-28=$0.00
- `12:04:39`   USW2-Requests-Tier1 after Aug-26: 08-26=$0.00 08-27=$0.00 08-28=$0.00
- `12:04:39`   USW2-Requests-Tier2 after Aug-26: 08-26=$0.00 08-27=$0.00 08-28=$0.00
- `12:04:39`   USW2-Retrieval-SIA after Aug-26: 08-26=$0.00 08-27=$0.00 08-28=$0.00
- `12:04:39`   USW2-TimedStorage-SIA-ByteHrs after Aug-26: 08-26=$0.93 08-27=$0.96 08-28=$0.00
- `12:04:39`   USW2-TimedStorage-SIA-SmObjects after Aug-26: 08-26=$0.09 08-27=$0.09 08-28=$0.00
- `12:04:39`   USW2-USE1-AWS-In-Bytes after Aug-26: 08-26=$0.00 08-27=$0.00 08-28=$0.00
- `12:04:39`   USW2-TimedStorage-ByteHrs after Aug-26: 08-26=$0.00 08-27=$0.00 08-28=$0.00
- `12:04:39`   top S3 operations on 2026-08-27:
- `12:04:39`     PutObject                  us-east-1  $  2.49  qty=      498349
- `12:04:39`     StandardStorage            us-east-1  $  1.80  qty=          78
- `12:04:39`     StandardIAStorage          us-west-2  $  0.96  qty=          76
- `12:04:39`     GetObject                  us-east-1  $  0.71  qty=      828064
- `12:04:39`     ListBucket                 us-east-1  $  0.43  qty=       86637
- `12:04:39`   VERDICT: STILL BURNING -- the Aug-26 fixes did not bend Requests-Tier1
- `12:04:39`   evidence -> data/ops/s3-cost-forensics.json
- `12:04:39` ops 5024 GREEN -- evidence banked; attribution armed (access logs stamp every request with the engine name from now on)
