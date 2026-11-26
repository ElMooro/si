# 6982 - the corporate bond desks' sources, beside a public chart of single-bond moves (read-only), 2026-11-26T20:42+00:00

**Status:** success  
**Duration:** 6.1s  
**Finished:** 2026-11-26T20:43:03+00:00  

## Data

| carried | chart_bonds | fred_asked | fred_live | sleeves_today |
|---|---|---|---|---|
| 22 | 24 | 24 | 23 | 23 |

## Log
## 1. The corporate bonds document today

- `20:42:58` {"written": "2026-11-26T20:15:30Z", "sleeves": 23, "fred_live": 23, "fred_asked": 24, "fred_failed_with": ["warehouse:"], "trace_live": 4, "trace_asked": 5, "funds_with_bonds": 16, "funds": 16, "holdings_day": "2026-11-25", "bonds": 23012, "ecb": "{'url': 'https://www.ecb.europa.eu/mopo/pdf/CSPP_PEPP_corporate_bond_holdings_20261120.csv", "cds": "joined 41 issuers from data/cds-desk.json"}
- `20:42:58` its own words on the US tape: FINRA TRACE corporate and Rule 144A market breadth, sentiment and capped volume. This is the whole disseminated US corporate tape rolled up by grade, not a sample. Individual CUSIPs are not in the public aggregate API.
- `20:42:58` TRACE corporate breadth as of 2026-11-25: [["all securities", 8092.0, 3280.0, 12172.0], ["convertibles", 113.0, 82.0, 207.0], ["high yield", 600.0, 719.0, 1732.0], ["investment grade", 7379.0, 2479.0, 10233.0]]
- `20:42:58` TRACE rule_144a breadth as of 2026-11-25: [["all securities", 1293.0, 1346.0, 2761.0], ["convertibles", 58.0, 47.0, 106.0], ["high yield", 518.0, 964.0, 1543.0], ["investment grade", 717.0, 335.0, 1112.0]]
- `20:42:58` TRACE dataset status: {"corporate_breadth": "live 32 rows", "corporate_sentiment": "live 288 rows", "m144a_breadth": "live 32 rows", "m144a_sentiment": "live 288 rows", "capped": "unavailable"}
- `20:42:58` funds: [["USIG", "2026-11-25", 11686, "api"], ["LQD", "2026-11-25", 3233, "api"], ["USHY", "2026-11-25", 1880, "api"], ["HYG", "2026-11-25", 1328, "api"], ["SHYG", "2026-11-25", 1145, "api"], ["FALN", "2026-11-25", 157, "api"], ["CEMB", "2026-11-25", 878, "api"], ["EMHY", "2026-11-25", 359, "api"], ["EUIG", "2026-11-25", 217, "api"], ["EUHY", "2026-11-25", 669, "api"], ["IGIL", "2026-11-25", 1694, "api"], ["ICVT", "2026-11-25", 391, "api"], ["FLOT", "2026-11-25", 529, "api"], ["IEAC", "2026-11-25", 4284, "api"], ["IAGG", "2026-11-25", 5701, "api"], ["IBND", "2026-11-24", 938, "live"]]
## 2. Earlier copies of the document (which source arrived on which day)

- `20:42:58` 11-26 20:16 | sleeves 23 | FRED 23 of 24 live ['warehouse:'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-25
- `20:42:58` 11-25 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-24
- `20:42:59` 11-24 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-23
- `20:42:59` 11-23 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-20
- `20:43:00` 11-20 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-19
- `20:43:00` 11-19 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-18
- `20:43:00` 11-18 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-17
- `20:43:01` 11-17 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-16
- `20:43:01` 11-16 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-13
- `20:43:01` 11-13 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 5 of 5 | funds 16 of 16 | holdings 2026-11-12
- `20:43:02` 11-12 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 5 of 5 | funds 16 of 16 | holdings 2026-11-11
- `20:43:02` 11-11 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 5 of 5 | funds 16 of 16 | holdings 2026-11-10
## 3. The Treasury par curve the credit board reads

- `20:43:02` par curve not read: -1
## 4. The chart's 24 bonds on our desks

- `20:43:03` bond desk: 23208 bonds, holdings of 2026-11-25 (price and yield moves are against the fund file before it) | credit board: 21097 bonds, as of 2026-11-24
- `20:43:03` HELD      CRWV 9 '31         COREWEAVE INC                      matures 2031-02-01 | price 74.61 (-7.11 since the month-end file) | yield 17.86 (2.91) | spread 1238.8 bp, week 193.4, month 212.6 | in 4 fund files, book US HY
- `20:43:03` HELD      CRWV 9.75 '31      COREWEAVE INC                      matures 2031-10-01 | price 74.63 (-6.81 since the month-end file) | yield 17.76 (2.52) | spread 1239.1 bp, week 172.7, month 185.9 | in 4 fund files, book US HY
- `20:43:03` HELD      CRWV 9.25 '30      COREWEAVE INC                      matures 2030-06-01 | price 79.17 (-6.01 since the month-end file) | yield 17.42 (2.71) | spread 1224.5 bp, week 201.1, month 220.3 | in 4 fund files, book US HY
- `20:43:03` HELD      CRWV 9.625 '32     COREWEAVE INC                      matures 2032-07-15 | price 73.37 (-6.99 since the month-end file) | yield 17.17 (2.34) | spread 1170.8 bp, week 153.8, month 162.0 | in 3 fund files, book US HY
- `20:43:03` HELD      EDGCOM 7.5 '31     EDGED COMPUTE LLC                  matures 2031-04-30 | price 82.54 (-3.38 since the month-end file) | yield 13.41 (-2.2) | spread 837.7 bp, week 65.6, month -266.3 | in 4 fund files, book US HY
- `20:43:03` HELD      CORZ 7.75 '31      CORE SCIENTIFIC FINANCE I LLC      matures 2031-05-15 | price 91.53 (-2.62 since the month-end file) | yield 10.15 (0.79) | spread 513.5 bp, week 72.0, month 47.1 | in 4 fund files, book US HY
- `20:43:03` NOT HELD  DDRIVE 8.875 '31    (same coupon and year among data-centre names: ['ZENITH ARC LLC'])
- `20:43:03` HELD      ELKGVP 7.5 '31     ELK GROVE VILLAGE PROPERTY LLC     matures 2031-06-15 | price 92.01 (-2.39 since the month-end file) | yield 9.98 (0.8) | spread 505.0 bp, week 54.1, month 55.3 | in 2 fund files, book US HY
- `20:43:03` HELD      APLD 9.25 '30      APLD COMPUTECO LLC                 matures 2030-12-15 | price 101.85 (-0.34 since the month-end file) | yield 8.35 (0.14) | spread 345.2 bp, week 26.1, month -15.1 | in 4 fund files, book US HY
- `20:43:03` HELD      GALAXY 9.875 '31   GALAXY HELIOS DATA CENTERS II LLC  matures 2031-08-01 | price 88.64 (-4.19 since the month-end file) | yield 13.19 (1.31) | spread 814.3 bp, week 35.4, month 96.6 | in 4 fund files, book US HY
- `20:43:03` HELD      ELNFOR 7 '31       APLD COMPUTECO 3 LLC               matures 2031-06-15 | price 91.84 (-1.59 since the month-end file) | yield 9.39 (0.51) | spread 463.7 bp, week 44.6, month 44.7 | in 4 fund files, book US HY
- `20:43:03` HELD      SECMOS 8.875 '31   SE COSMOS LLC                      matures 2031-05-01 | price 93.15 (-3.71 since the month-end file) | yield 10.86 (1.11) | spread 598.6 bp, week 51.0, month 92.9 | in 2 fund files, book US HY
- `20:43:03` HELD      ZENARC 8.875 '31   ZENITH ARC LLC                     matures 2031-08-31 | price 82.4 (-2.42 since the month-end file) | yield 14.05 (0.84) | spread 897.2 bp, week 44.6, month 46.7 | in 4 fund files, book US HY
- `20:43:03` HELD      TRACTC 5.875 '31   SV RNO PROPERTY OWNER 1 LLC        matures 2031-03-01 | price 82.69 (-4.1 since the month-end file) | yield 11.07 (1.4) | spread 610.7 bp, week 95.6, month 112.7 | in 4 fund files, book US HY
- `20:43:03` HELD      PFORGE 6.75 '31    APLD COMPUTECO 2 LLC               matures 2031-03-15 | price 93.13 (-2.49 since the month-end file) | yield 8.92 (0.83) | spread 409.1 bp, week 45.9, month 68.0 | in 4 fund files, book US HY
- `20:43:03` HELD      TRACTD 6.5 '31     PR RNO PROPERTY OWNER 1 LLC        matures 2031-05-01 | price 84.45 (-4.52 since the month-end file) | yield 11.12 (1.49) | spread 635.6 bp, week 90.0, month 142.1 | in 4 fund files, book US HY
- `20:43:03` HELD      YNDRDC 6.875 '31   YONDR JK 1 LLC                     matures 2031-06-30 | price 87.0 (-3.46 since the month-end file) | yield 10.83 (1.17) | spread 612.6 bp, week 46.8, month 115.8 | in 2 fund files, book US HY
- `20:43:03` HELD      FLASHC 7.25 '30    FLASH COMPUTE LLC                  matures 2030-12-31 | price 97.16 (-1.03 since the month-end file) | yield 8.07 (0.31) | spread 324.9 bp, week 18.2, month 17.4 | in 4 fund files, book US HY
- `20:43:03` HELD      MERIDI 6.25 '31    MERIDIAN ARC HOLDCO LLC            matures 2031-04-30 | price 90.38 (-0.99 since the month-end file) | yield 8.92 (0.32) | spread 410.6 bp, week 36.9, month 20.1 | in 4 fund files, book US HY
- `20:43:03` HELD      CIFR 7.125 '30     CIPHER COMPUTE LLC                 matures 2030-11-15 | price 98.9 (-0.63 since the month-end file) | yield 7.45 (0.07) | spread 273.0 bp, week 22.9, month -9.7 | in 4 fund files, book US HY
- `20:43:03` HELD      WULF 7.75 '30      WULF COMPUTE LLC                   matures 2030-10-15 | price 102.07 (-0.34 since the month-end file) | yield 6.94 (0.11) | spread 222.1 bp, week 23.8, month 6.9 | in 4 fund files, book US HY
- `20:43:03` NOT HELD  CLSPRK 7.875 '31  
- `20:43:03` HELD      STNGRY 6 '31       STINGRAY COMPUTE LLC               matures 2031-06-15 | price 94.32 (-0.99 since the month-end file) | yield 7.49 (0.28) | spread 278.4 bp, week 29.1, month 27.3 | in 2 fund files, book US HY
- `20:43:03` HELD      BLKPRL 6.125 '31   BLACK PEARL COMPUTE LLC            matures 2031-02-15 | price 97.33 (-0.73 since the month-end file) | yield 6.86 (0.21) | spread 216.8 bp, week 26.5, month 20.7 | in 4 fund files, book US HY
- `20:43:03` carried: 22 of the chart's 24 bonds
- `20:43:03` every data-centre / compute name in the dollar books (24): {"AMERICAN BUILDERS & CONTRACTORS SU": ["4.0 '28", "3.88 '29"], "APLD COMPUTECO 2 LLC": ["6.75 '31"], "APLD COMPUTECO 3 LLC": ["7.0 '31"], "APLD COMPUTECO LLC": ["9.25 '30"], "BEACON POINT DC LLC": ["6.13 '42"], "BLACK PEARL COMPUTE LLC": ["6.13 '31"], "CHILDRENS HOSPITAL/DC": ["2.93 '50"], "CIPHER COMPUTE LLC": ["7.13 '30"], "CORE SCIENTIFIC FINANCE I LLC": ["7.75 '31"], "COREWEAVE INC": ["9.75 '31", "9.25 '30", "9.0 '31", "9.63 '32"], "EDGED COMPUTE LLC": ["7.5 '31"], "FLASH COMPUTE LLC": ["7.25 '30"], "GALAXY HELIOS DATA CENTERS II LLC": ["9.88 '31"], "GALAXY PIPELINE ASSETS BID CO LTD": ["2.63 '36", "3.25 '40"], "GALAXY PIPELINE ASSETS BIDCO LTD": ["2.94 '40", "2.16 '34"], "HUT 8 DC LLC": ["6.19 '42"], "MERIDIAN ARC HOLDCO LLC": ["6.25 '31"], "PR RNO PROPERTY OWNER 1 LLC": ["6.5 '31"], "STINGRAY COMPUTE LLC": ["6.0 '31"], "SV RNO PROPERTY OWNER 1 LLC": ["5.88 '31"], "TRACTOR SUPPLY COMPANY": ["1.75 '30", "5.25 '33", "5.2 '32"], "WULF COMPUTE LLC": ["7.75 '30"], "YONDR JK 1 LLC": ["6.88 '31"], "ZENITH ARC LLC": ["8.88 '31"]}
- `20:43:03` ✅ read
