## P1 worldbank failure bodies

**Status:** success  
**Duration:** 6.6s  
**Finished:** 2026-08-25T14:41:21+00:00  

## Data

| polygon_key | wb_banked | wb_failures |
|---|---|---|
| True | 9200 | 1 |

## Log
- `14:41:15`   banked=9200 q=20095 failures=1
- `14:41:15`   FAIL GD_WBL_MAR_LAW_DIVORCE {"err": "HTTP 400", "tries": 3}
- `14:41:15`   engine URL shape: https://api.worldbank.org/v2
- `14:41:18`   retest GD_WBL_MAR_LAW_DIVORCE -> 200 13881B head=b'PK\x03\x04\x14\x00\x00\x00\x08\x00(U\x19]\xf91\x92+\x1c\x02\x00\x00\x9a\x03\x00\x00G\x00\x1c\x00Metadata_Indicator_API_GD_WBL_MAR_LAW_DIVORCE_DS14_en_csv_v2_211910.csv \xa2\x18\x00(\xa0\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
## P2 polygon window v2

- `14:41:18`   AAPL 2019 -> 403 results=0 {"status":"NOT_AUTHORIZED","request_id":"fa4a4648e1887806c8e6e55df39c93d1","mess
- `14:41:19`   AAPL 2021 -> 403 results=0 {"status":"NOT_AUTHORIZED","request_id":"651376536dbee8ba566ac49e76bdbb9a","mess
- `14:41:19`   AAPL 2022 -> 200 results=5 
- `14:41:19`   AAPL 2023 -> 200 results=5 
- `14:41:20`   AAPL 2024 -> 200 results=5 
- `14:41:20`   AAPL 2025 -> 200 results=5 
- `14:41:21`   grouped(no-limit) 2024-06-03 -> 200 results=10561 bytes=1088793
- `14:41:21` ops 4975 GREEN -- forensics banked; fixes follow the evidence
