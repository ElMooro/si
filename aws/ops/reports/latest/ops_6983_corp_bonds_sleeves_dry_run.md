# 6983 - Corp bonds: the ICE sleeves from the warehouse, dry run on the runner (nothing written), 2026-11-26T20:01+00:00

**Status:** success  
**Duration:** 48.4s  
**Finished:** 2026-11-26T20:02:12+00:00  

## Data

| failed_checks | from_fred | from_warehouse | seconds | sleeves |
|---|---|---|---|---|
| 0 | 0 | 23 | 35.6 | 23 |

## Log
## 1. The engine's own function

- `20:01:59` 23 of 24 sleeves in 35.6 s | 23 from the warehouse, 0 from FRED | not read: {"BAMLSE00ECLIOAS": "warehouse: HTTPError; FRED: TimeoutError The read operation timed out"}
- `20:01:59`   US IG                BAMLC0A0CM      97.0 bp  as of 2026-11-23 | 1d -1.0, 5d 7.0, 21d 8.0 | warehouse
- `20:01:59`   US AAA               BAMLC0A1CA      49.0 bp  as of 2026-11-24 | 1d 1.0, 5d 2.0, 21d 5.0 | warehouse
- `20:01:59`   US AA                BAMLC0A2CA      70.0 bp  as of 2026-11-23 | 1d -2.0, 5d 5.0, 21d 5.0 | warehouse
- `20:01:59`   US A                 BAMLC0A3CA      81.0 bp  as of 2026-11-24 | 1d 1.0, 5d 5.0, 21d 5.0 | warehouse
- `20:01:59`   US BBB               BAMLC0A4CB     120.0 bp  as of 2026-11-23 | 1d -2.0, 5d 8.0, 21d 9.0 | warehouse
- `20:01:59`   US HY                BAMLH0A0HY     363.0 bp  as of 2026-11-23 | 1d -13.0, 5d 29.0, 21d 23.0 | warehouse
- `20:01:59`   US BB                BAMLH0A1HY     225.0 bp  as of 2026-11-24 | 1d 6.0, 5d 21.0, 21d 19.0 | warehouse
- `20:01:59`   US B                 BAMLH0A2HY     405.0 bp  as of 2026-11-24 | 1d 12.0, 5d 37.0, 21d 62.0 | warehouse
- `20:01:59`   US CCC               BAMLH0A3HY    1405.0 bp  as of 2026-11-24 | 1d 18.0, 5d 76.0, 21d 49.0 | warehouse
- `20:01:59`   US IG yield          BAMLC0A0CM      5.83 pct as of 2026-11-23 | 1d -0.01, 5d -0.12, 21d 0.04 | warehouse
- `20:01:59`   US HY yield          BAMLH0A0HY      8.36 pct as of 2026-11-23 | 1d -0.1, 5d 0.11, 21d 0.22 | warehouse
- `20:01:59`   US IG 1-3Y           BAMLC1A0C1      65.0 bp  as of 2026-11-23 | 1d -1.0, 5d 4.0, 21d 6.0 | warehouse
- `20:01:59`   US IG 3-5Y           BAMLC2A0C3      87.0 bp  as of 2026-11-23 | 1d -1.0, 5d 7.0, 21d 8.0 | warehouse
- `20:01:59`   US IG 5-7Y           BAMLC3A0C5     102.0 bp  as of 2026-11-23 | 1d -1.0, 5d 9.0, 21d 10.0 | warehouse
- `20:01:59`   US IG 7-10Y          BAMLC4A0C7     117.0 bp  as of 2026-11-23 | 1d -1.0, 5d 9.0, 21d 9.0 | warehouse
- `20:01:59`   US IG 10-15Y         BAMLC7A0C1     116.0 bp  as of 2026-11-23 | 1d 0.0, 5d 8.0, 21d 7.0 | warehouse
- `20:01:59`   US IG 15Y+           BAMLC8A0C1     115.0 bp  as of 2026-11-23 | 1d -1.0, 5d 7.0, 21d 8.0 | warehouse
- `20:01:59`   Euro HY              BAMLHE00EH     356.0 bp  as of 2026-11-24 | 1d 4.0, 5d 19.0, 21d -8.0 | warehouse
- `20:01:59`   Euro HY yield        BAMLHE00EH      6.65 pct as of 2026-11-23 | 1d -0.04, 5d 0.17, 21d -0.04 | warehouse
- `20:01:59`   EM corporate         BAMLEMCBPI     196.0 bp  as of 2026-11-23 | 1d -2.0, 5d 22.0, 21d 23.0 | warehouse
- `20:01:59`   EM high grade        BAMLEMIBHG     110.0 bp  as of 2026-11-24 | 1d 0.0, 5d 9.0, 21d 8.0 | warehouse
- `20:01:59`   Asia USD             BAMLEMALLC     123.0 bp  as of 2026-11-24 | 1d 1.0, 5d 10.0, 21d 13.0 | warehouse
- `20:01:59`   EM euro yield        BAMLEMEBCR      4.78 pct as of 2026-11-24 | 1d 0.0, 5d 0.02, 21d 0.04 | warehouse
- `20:01:59` PASS  nothing was written or read from the bucket by the sleeves (no call)
- `20:01:59` PASS  the sleeves are back: 23 of 24, each with the status the page reads as printed
- `20:01:59` PASS  inside the 150 seconds set aside (35.6 s), one request at a time
- `20:01:59` PASS  the unit is right: US IG 97.0 bp under US HY 363.0 bp under CCC 1405.0 bp
- `20:01:59` PASS  a yield sleeve stays in percent (US HY yield 8.36)
- `20:01:59` PASS  the sleeves are current: none more than a week old but none
- `20:01:59` newest day across the sleeves: 2026-11-24 | oldest: 2026-11-23
## 2. FRED's graph file, asked once from the runner (information)

- `20:02:12` did not answer in 12.1 s: TimeoutError The read operation timed out
- `20:02:12` ✅ the sleeves read from the warehouse
