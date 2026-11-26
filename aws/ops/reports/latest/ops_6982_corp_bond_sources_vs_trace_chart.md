# 6982 - the corporate bond desks' sources, beside a public chart of single-bond moves (read-only), 2026-11-26T19:40+00:00

**Status:** success  
**Duration:** 4.1s  
**Finished:** 2026-11-26T19:40:15+00:00  

## Data

| carried | chart_bonds | fred_asked | fred_live | sleeves_today |
|---|---|---|---|---|
| 11 | 24 | 24 | 0 | 0 |

## Log
## 1. The corporate bonds document today

- `19:40:11` {"written": "2026-11-25T20:15:30Z", "sleeves": 0, "fred_live": 0, "fred_asked": 24, "fred_failed_with": ["TimeoutError"], "trace_live": 4, "trace_asked": 5, "funds_with_bonds": 16, "funds": 16, "holdings_day": "2026-11-24", "bonds": 23029, "ecb": "{'url': 'https://www.ecb.europa.eu/mopo/pdf/CSPP_PEPP_corporate_bond_holdings_20261120.csv", "cds": "joined 20 issuers from data/cds-desk.json"}
- `19:40:11` its own words on the US tape: FINRA TRACE corporate and Rule 144A market breadth, sentiment and capped volume. This is the whole disseminated US corporate tape rolled up by grade, not a sample. Individual CUSIPs are not in the public aggregate API.
- `19:40:11` TRACE corporate breadth as of 2026-11-24: [["all securities", 5890.0, 6471.0, 13413.0], ["convertibles", 95.0, 103.0, 210.0], ["high yield", 706.0, 888.0, 2187.0], ["investment grade", 5089.0, 5480.0, 11016.0]]
- `19:40:11` TRACE rule_144a breadth as of 2026-11-24: [["all securities", 1075.0, 2092.0, 3319.0], ["convertibles", 40.0, 63.0, 107.0], ["high yield", 405.0, 1272.0, 1730.0], ["investment grade", 630.0, 757.0, 1482.0]]
- `19:40:11` TRACE dataset status: {"corporate_breadth": "live 32 rows", "corporate_sentiment": "live 288 rows", "m144a_breadth": "live 32 rows", "m144a_sentiment": "live 288 rows", "capped": "unavailable"}
- `19:40:11` funds: [["USIG", "2026-11-24", 11696, "api"], ["LQD", "2026-11-24", 3236, "api"], ["USHY", "2026-11-24", 1881, "api"], ["HYG", "2026-11-24", 1328, "api"], ["SHYG", "2026-11-24", 1145, "api"], ["FALN", "2026-11-24", 157, "api"], ["CEMB", "2026-11-24", 882, "api"], ["EMHY", "2026-11-24", 363, "api"], ["EUIG", "2026-11-24", 217, "api"], ["EUHY", "2026-11-24", 668, "api"], ["IGIL", "2026-11-24", 1694, "api"], ["ICVT", "2026-11-24", 394, "api"], ["FLOT", "2026-11-24", 530, "api"], ["IEAC", "2026-11-24", 4282, "api"], ["IAGG", "2026-11-24", 5698, "api"], ["IBND", "2026-11-24", 938, "live"]]
## 2. Earlier copies of the document (which source arrived on which day)

- `19:40:11` 11-25 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-24
- `19:40:12` 11-24 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-23
- `19:40:12` 11-23 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-20
- `19:40:12` 11-20 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-19
- `19:40:12` 11-19 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-18
- `19:40:13` 11-18 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-17
- `19:40:13` 11-17 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-16
- `19:40:13` 11-16 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 4 of 5 | funds 16 of 16 | holdings 2026-11-13
- `19:40:13` 11-13 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 5 of 5 | funds 16 of 16 | holdings 2026-11-12
- `19:40:14` 11-12 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 5 of 5 | funds 16 of 16 | holdings 2026-11-11
- `19:40:14` 11-11 20:16 | sleeves 0 | FRED 0 of 24 live ['TimeoutError'] | TRACE 5 of 5 | funds 16 of 16 | holdings 2026-11-10
## 3. The Treasury par curve the credit board reads

- `19:40:14` par curve not read: -1
## 4. The chart's 24 bonds on our desks

- `19:40:14` bond desk: 23202 bonds, holdings of 2026-11-24 (price and yield moves are against the fund file before it) | credit board: 21097 bonds, as of 2026-11-24
- `19:40:14` HELD      CRWV 9 '31         COREWEAVE INC                      matures 2031-02-01 | price 76.49 (-5.23 on the day) | yield 17.07 (2.12) | spread 1238.8 bp, week 193.4, month 212.6 | in 4 fund files, book US HY
- `19:40:14` HELD      CRWV 9.75 '31      COREWEAVE INC                      matures 2031-10-01 | price 76.42 (-5.02 on the day) | yield 17.08 (1.84) | spread 1239.1 bp, week 172.7, month 185.9 | in 4 fund files, book US HY
- `19:40:14` HELD      CRWV 9.25 '30      COREWEAVE INC                      matures 2030-06-01 | price 80.26 (-4.92 on the day) | yield 16.92 (2.21) | spread 1224.5 bp, week 201.1, month 220.3 | in 4 fund files, book US HY
- `19:40:14` NOT HELD  CRWV 9.625 '32    
- `19:40:14` HELD      EDGCOM 7.5 '31     EDGED COMPUTE LLC                  matures 2031-04-30 | price 83.44 (-2.48 on the day) | yield 13.06 (-2.55) | spread 837.7 bp, week 65.6, month -266.3 | in 4 fund files, book US HY
- `19:40:14` HELD      CORZ 7.75 '31      CORE SCIENTIFIC FINANCE I LLC      matures 2031-05-15 | price 92.64 (-1.51 on the day) | yield 9.82 (0.46) | spread 513.5 bp, week 72.0, month 47.1 | in 4 fund files, book US HY
- `19:40:14` NOT HELD  DDRIVE 8.875 '31  
- `19:40:14` HELD      ELKGVP 7.5 '31     ELK GROVE VILLAGE PROPERTY LLC     matures 2031-06-15 | price 92.77 (-1.63 on the day) | yield 9.73 (0.55) | spread 505.0 bp, week 54.1, month 55.3 | in 2 fund files, book US HY
- `19:40:14` HELD      APLD 9.25 '30      APLD COMPUTECO LLC                 matures 2030-12-15 | price 102.38 (0.19 on the day) | yield 8.1 (-0.11) | spread 345.2 bp, week 26.1, month -15.1 | in 4 fund files, book US HY
- `19:40:14` NOT HELD  GALAXY 9.875 '31  
- `19:40:14` NOT HELD  ELNFOR 7 '31        (same coupon and year among data-centre names: ['APLD COMPUTECO 3 LLC'])
- `19:40:14` NOT HELD  SECMOS 8.875 '31  
- `19:40:14` NOT HELD  ZENARC 8.875 '31  
- `19:40:14` NOT HELD  TRACTC 5.875 '31  
- `19:40:14` NOT HELD  PFORGE 6.75 '31     (same coupon and year among data-centre names: ['APLD COMPUTECO 2 LLC'])
- `19:40:14` NOT HELD  TRACTD 6.5 '31      (same coupon and year among data-centre names: ['PR RNO PROPERTY OWNER 1 LLC'])
- `19:40:14` NOT HELD  YNDRDC 6.875 '31  
- `19:40:14` HELD      FLASHC 7.25 '30    FLASH COMPUTE LLC                  matures 2030-12-31 | price 97.63 (-0.56 on the day) | yield 7.93 (0.17) | spread 324.9 bp, week 18.2, month 17.4 | in 4 fund files, book US HY
- `19:40:14` HELD      MERIDI 6.25 '31    MERIDIAN ARC HOLDCO LLC            matures 2031-04-30 | price 90.84 (-0.53 on the day) | yield 8.79 (0.19) | spread 410.6 bp, week 36.9, month 20.1 | in 4 fund files, book US HY
- `19:40:14` NOT HELD  CIFR 7.125 '30    
- `19:40:14` HELD      WULF 7.75 '30      WULF COMPUTE LLC                   matures 2030-10-15 | price 102.19 (-0.22 on the day) | yield 6.9 (0.07) | spread 222.1 bp, week 23.8, month 6.9 | in 4 fund files, book US HY
- `19:40:14` NOT HELD  CLSPRK 7.875 '31  
- `19:40:14` HELD      STNGRY 6 '31       STINGRAY COMPUTE LLC               matures 2031-06-15 | price 94.4 (-0.91 on the day) | yield 7.47 (0.26) | spread 278.4 bp, week 29.1, month 27.3 | in 2 fund files, book US HY
- `19:40:14` NOT HELD  BLKPRL 6.125 '31  
- `19:40:14` carried: 11 of the chart's 24 bonds
- `19:40:15` every data-centre / compute name in the dollar books (24): {"AMERICAN BUILDERS & CONTRACTORS SU": ["4.0 '28", "3.88 '29"], "APLD COMPUTECO 2 LLC": ["6.75 '31"], "APLD COMPUTECO 3 LLC": ["7.0 '31"], "APLD COMPUTECO LLC": ["9.25 '30"], "BEACON POINT DC LLC": ["6.13 '42"], "BLACK PEARL COMPUTE LLC": ["6.13 '31"], "CHILDRENS HOSPITAL/DC": ["2.93 '50"], "CIPHER COMPUTE LLC": ["7.13 '30"], "CORE SCIENTIFIC FINANCE I LLC": ["7.75 '31"], "COREWEAVE INC": ["9.75 '31", "9.25 '30", "9.0 '31", "9.63 '32"], "EDGED COMPUTE LLC": ["7.5 '31"], "FLASH COMPUTE LLC": ["7.25 '30"], "GALAXY HELIOS DATA CENTERS II LLC": ["9.88 '31"], "GALAXY PIPELINE ASSETS BID CO LTD": ["2.63 '36", "3.25 '40"], "GALAXY PIPELINE ASSETS BIDCO LTD": ["2.94 '40", "2.16 '34"], "HUT 8 DC LLC": ["6.19 '42"], "MERIDIAN ARC HOLDCO LLC": ["6.25 '31"], "PR RNO PROPERTY OWNER 1 LLC": ["6.5 '31"], "STINGRAY COMPUTE LLC": ["6.0 '31"], "SV RNO PROPERTY OWNER 1 LLC": ["5.88 '31"], "TRACTOR SUPPLY COMPANY": ["1.75 '30", "5.25 '33", "5.2 '32"], "WULF COMPUTE LLC": ["7.75 '30"], "YONDR JK 1 LLC": ["6.88 '31"], "ZENITH ARC LLC": ["8.88 '31"]}
- `19:40:15` ✅ read
