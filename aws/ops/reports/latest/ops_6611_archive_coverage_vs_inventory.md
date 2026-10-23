# ops 6611 - archive coverage against the live bucket's S3 Inventory (read-only)

**Status:** success  
**Duration:** 142.6s  
**Finished:** 2026-10-23T00:14:00+00:00  

## Data

| current_objects | in_archive | inventory_stamp | missing_in_archive | missing_pct | sampled |
|---|---|---|---|---|---|
| 3420242 | 3109 | 2026-10-22T01-00Z | 0 | 0.0 | 3109 |

## Log
## d. CloudWatch storage metrics, last days

- `00:11:38` justhodl-dashboard-live: {"NumberOfObjects": [["10-18", 3966082], ["10-19", 3981806], ["10-20", 3807457], ["10-21", 3887848]], "BucketSizeBytes/StandardStorage": [["10-18", 887.1], ["10-19", 887.7], ["10-20", 864.3], ["10-21", 875.3]], "BucketSizeBytes/StandardIAStorage": [["10-18", 172.6], ["10-19", 172.6], ["10-20", 172.6], ["10-21", 172.6]]}
- `00:11:39` justhodl-warehouse-archive-857687956942: {"NumberOfObjects": [["10-21", 1363410]], "BucketSizeBytes/StandardStorage": [], "BucketSizeBytes/StandardIAStorage": [["10-21", 446.0]]}
## a. current objects by prefix (inventory 2026-10-22T01-00Z)

- `00:12:19` current objects 3420242; top-level prefixes by objects: [["data/", 2807121, 916.1], ["audit-private/", 517762, 79.8], ["equity-research/", 32951, 10.6], ["equity-research-history/", 24187, 2.8], ["lake/", 10131, 0.0], ["archive/", 7284, 0.3], ["history/", 6381, 2.4], ["edgar-insiders/", 4795, 0.0], ["index/", 2581, 15.2], ["llm-cache/", 2500, 0.0], ["errors/", 2251, 0.0], ["etf-flows/", 539, 0.6], ["13f-cache/", 209, 0.1], ["screener/", 168, 0.0], ["calibration/", 146, 0.0], ["system-events/", 139, 0.1], ["flow-anomalies/", 139, 0.0], ["macro/", 137, 0.0], ["etf-constituents-v2/", 131, 0.0], ["equity-prewarm/", 126, 0.0]]
- `00:12:19` data/ second level by objects (top 30): [["data/providers/", 1137345, 293.8], ["data/warm/", 778761, 207.4], ["data/evidence/", 173418, 1.0], ["data/etf-holdings-research/", 111612, 8.6], ["data/option-flow-research/", 95102, 43.0], ["data/series-cache/", 67399, 1.9], ["data/fred-levels/", 60213, 0.1], ["data/archive/", 46557, 41.0], ["data/fundgraph/", 38916, 6.7], ["data/option-population-research/", 36475, 8.0], ["data/holdings-research/", 23415, 75.3], ["data/statement-research/", 13598, 3.1], ["data/price-observations/", 12235, 0.0], ["data/research-forecasts/", 9446, 0.3], ["data/symdir/", 9346, 1.1], ["data/provider-flow-research/", 8692, 0.7], ["data/bond-vol-research/", 8199, 24.6], ["data/carry-research/", 7897, 3.3], ["data/momentum-leaders/", 7725, 1.1], ["data/vintage-research/", 7125, 6.1], ["data/etf-desk-research/", 6604, 0.4], ["data/raw/", 6180, 3.0], ["data/ops/", 5792, 0.0], ["data/research-network/", 5592, 0.9], ["data/options-flow-scanner/", 5526, 0.1], ["data/short-volume-research/", 3893, 1.1], ["data/risk-gate-research/", 3829, 14.9], ["data/global-stress-research/", 3686, 0.5], ["data/crypto-funding-research/", 3436, 0.1], ["data/crypto-stablecoin-research/", 3423, 4.1]]
- `00:12:19` data/ second level by GB (top 15): [["data/providers/", 293.8, 1137345], ["data/warm/", 207.4, 778761], ["data/holdings-research/", 75.3, 23415], ["data/index/", 60.9, 3071], ["data/option-flow-research/", 43.0, 95102], ["data/archive/", 41.0, 46557], ["data/search/", 35.1, 1135], ["data/bond-vol-research/", 24.6, 8199], ["data/risk-gate-research/", 14.9, 3829], ["data/crisis-research/", 9.0, 1812], ["data/etf-holdings-research/", 8.6, 111612], ["data/option-population-research/", 8.0, 36475], ["data/fundgraph/", 6.7, 38916], ["data/vintage-research/", 6.1, 7125], ["data/xbrl-raw/", 5.6, 3323]]
## b. sample of 3109 current keys HEADed in the archive

- `00:12:25` in the archive: 3109 of 3109 sampled (100.0%); storage classes {"STANDARD_IA": 3109}
- `00:12:25` by prefix (found, missing): [["data/providers/", [1035, 0]], ["data/warm/", [708, 0]], ["audit-private/", [471, 0]], ["data/evidence/", [157, 0]], ["data/etf-holdings-research/", [102, 0]], ["data/option-flow-research/", [86, 0]], ["data/series-cache/", [61, 0]], ["data/fred-levels/", [55, 0]], ["data/archive/", [42, 0]], ["data/fundgraph/", [35, 0]], ["data/option-population-research/", [33, 0]], ["equity-research/", [30, 0]], ["equity-research-history/", [22, 0]], ["data/holdings-research/", [21, 0]], ["data/statement-research/", [12, 0]], ["data/price-observations/", [12, 0]], ["data/research-forecasts/", [9, 0]], ["data/symdir/", [9, 0]], ["lake/", [9, 0]], ["data/vintage-research/", [7, 0]], ["data/bond-vol-research/", [7, 0]], ["data/carry-research/", [7, 0]], ["data/momentum-leaders/", [7, 0]], ["data/provider-flow-research/", [7, 0]], ["archive/", [6, 0]], ["data/etf-desk-research/", [6, 0]], ["history/", [6, 0]], ["data/ops/", [6, 0]], ["data/raw/", [5, 0]], ["data/research-network/", [5, 0]], ["data/options-flow-scanner/", [5, 0]], ["data/risk-gate-research/", [4, 0]], ["data/short-volume-research/", [4, 0]], ["data/global-stress-research/", [4, 0]], ["edgar-insiders/", [4, 0]], ["data/sector-capital-research/", [3, 0]], ["data/", [3, 0]], ["data/xbrl-fundamentals/", [3, 0]], ["data/xbrl-raw/", [3, 0]], ["data/backlog/", [3, 0]]]
- `00:12:25` missing examples: []
## c. bounded archive counts for the ten biggest data/ sub-prefixes

- `00:12:35` data/providers/                               inventory  1137345 | archive    60000 (listing stopped at 60 pages)
- `00:12:47` data/warm/                                    inventory   778761 | archive    60000 (listing stopped at 60 pages)
- `00:12:58` data/evidence/                                inventory   173418 | archive    60000 (listing stopped at 60 pages)
- `00:13:09` data/etf-holdings-research/                   inventory   111612 | archive    60000 (listing stopped at 60 pages)
- `00:13:19` data/option-flow-research/                    inventory    95102 | archive    60000 (listing stopped at 60 pages)
- `00:13:29` data/series-cache/                            inventory    67399 | archive    60000 (listing stopped at 60 pages)
- `00:13:39` data/fred-levels/                             inventory    60213 | archive    60000 (listing stopped at 60 pages)
- `00:13:47` data/archive/                                 inventory    46557 | archive    46565
- `00:13:54` data/fundgraph/                               inventory    38916 | archive    38997
- `00:14:00` data/option-population-research/              inventory    36475 | archive    36801
- `00:14:00` ✅ every sampled current object is in the archive
- `00:14:00` ✅ read-only; nothing written, copied or deleted
