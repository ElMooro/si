# ops 6925 - the dollar-shortage document with the Federal Reserve's series, dry run (nothing written), 2026-11-22T21:48+00:00

**Status:** success  
**Duration:** 19.0s  
**Finished:** 2026-11-22T21:49:07+00:00  

## Data

| bytes | failed_checks | gzip | measures | observations | requests | seconds |
|---|---|---|---|---|---|---|
| 1021829 | 0 | 256427 | 15 | 46623 | 19 | 17.5 |

## Log
## 1. The run

- `21:49:06` receipt: {"published": true, "generated_at": "2026-11-22T21:48:49.170564+00:00", "desk_generated_at": "2026-11-22T16:39:59.412623+00:00", "coverage": {"measures": 15, "observations": 46623, "ecb": 3, "fed": 12, "errors": 0, "status": "measured"}, "series_asked": 19, "latest": {"FED_SWAPS": [0.146, "2026-11-18"], "FED_FIMA": [0.001, "2026-11-18"], "AUX_FX_EA": [20.193, "2026-11-13"], "USD_DXY": [103.3578, "2026-11-13"], "USD_BROAD": [122.6503, "2026-11-13"], "USD_AFE": [116.236, "2026-11-13"], "USD_EME": [130.8877, "2026-11-13"], "AUX_EURUSD": [1.099, "2026-11-20"], "FED_CUSTODY_UST": [2544.011, "2026-11-18"], "FED_CUSTODY_ALL": [2813.231, "2026-11-18"], "FED_FOREIGN_RRP": [316.712, "2026-11-18"], "USD_SOFR_IOR": [1.0, "2026-11-19"], "USD_TED": [9.0, "2022-01-21"], "H8_NET_DUE": [885.647, "2026-11-11"], "AUX_FX_NONEA": [521.892, "2026-11-13"]}, "seconds": 17.5}
- `21:49:06`   asked fred%3ASWPT                    0.6s    26303 bytes | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, 
- `21:49:06`   asked fred%3AH41RESPPALGTRFNWW       0.5s    24265 bytes | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, 
- `21:49:06`   asked fred%3ADEXUSEU                 0.2s   153233 bytes | warehouse:fred-scoped/Exchange_Rates
- `21:49:06`   asked fred%3ADEXJPUS                 0.5s   303885 bytes | warehouse:fred-scoped/Exchange_Rates
- `21:49:06`   asked fred%3ADEXUSUK                 0.4s   304501 bytes | warehouse:fred-scoped/Exchange_Rates
- `21:49:06`   asked fred%3ADEXCAUS                 0.4s   306492 bytes | warehouse:fred-scoped/Exchange_Rates
- `21:49:06`   asked fred%3ADEXSDUS                 0.4s   302653 bytes | warehouse:fred-scoped/Exchange_Rates
- `21:49:06`   asked fred%3ADEXSZUS                 0.5s   304475 bytes | warehouse:fred-scoped/Exchange_Rates
- `21:49:06`   asked fred%3ADTWEXBGS                0.4s   123673 bytes | warehouse:fred-scoped/Exchange_Rates
- `21:49:06`   asked fred%3ADTWEXAFEGS              0.3s   123167 bytes | warehouse:fred-scoped/Exchange_Rates
- `21:49:06`   asked fred%3ADTWEXEMEGS              0.3s   123474 bytes | warehouse:fred-scoped/Exchange_Rates
- `21:49:06`   asked fred%3AWMTSECL1                0.2s    30237 bytes | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, 
- `21:49:06`   asked fred%3AWSEFINTL1               0.3s    30263 bytes | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, 
- `21:49:06`   asked fred%3AWLRRAFOIAL              0.2s    29873 bytes | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, 
- `21:49:06`   asked fred%3ASOFR                    0.2s    43167 bytes | warehouse:fred-scoped/Interest_Rates
- `21:49:06`   asked fred%3AIORB                    0.2s    38085 bytes | warehouse:fred-scoped/Interest_Rates
- `21:49:06`   asked fred%3AIOER                    0.3s    92461 bytes | warehouse:fred-scoped/On_Demand
- `21:49:06`   asked fred%3ATEDRATE                 0.2s   176442 bytes | warehouse:fred-scoped/On_Demand
- `21:49:06`   asked fred%3ANDFFRIW027NBOG          0.4s    63949 bytes | warehouse:fred-scoped/Banking (+15 obs tail from FRED, bank 
- `21:49:06` 17.5s | read from the bucket: ['ciss-sovereign.json', '84b666b398336f6dacbf82bf61f7', '9284342b95606f5b57be9b0ba898', 'cd31e81c2b30ff9bcb94f96cadea'] | kept in memory, not written: ['data/ciss-research/sovereign/dollar-state.json', 'data/ciss-dollar-shortage.json']
- `21:49:06` document ciss-dollar-shortage.v1 1.1.0 | 1021829 bytes, 256427 gzip | kept series 1905851 bytes, 391883 gzip | stands on the desk document of 2026-11-22T16:39
## 2. The three ECB measures are the desk document's own, as before

- `21:49:06` AUX_EURUSD    1.099 usd on 2026-11-20 | 7141 observations since 1999-01-04 | part price | called "EUR/USD"
- `21:49:06` AUX_FX_EA     20.193 eur_bn on 2026-11-13 | 1455 observations since 1999-01-01 | part backstop | called "Eurosystem foreign-currency lending to euro-area banks"
- `21:49:06` AUX_FX_NONEA  521.892 eur_bn on 2026-11-13 | 1455 observations since 1999-01-01 | part reserves | called "Eurosystem foreign reserves other than gold"
- `21:49:06` the reserves item's largest one-week changes: +121.434 bn in the week to 2021-08-27 (355.705 -> 477.139) | +38.833 bn in the week to 2009-08-28 (158.416 -> 197.249) | +38.587 bn in the week to 1999-04-02 (228.549 -> 267.136) | -28.989 bn in the week to 2022-12-30 (552.206 -> 523.217) | -26.381 bn in the week to 2025-07-04 (515.435 -> 489.054) | -25.213 bn in the week to 2002-06-28 (263.172 -> 237.959)
## 3. The twelve Federal Reserve measures

- `21:49:06` Dollars lent by central banks - What the Federal Reserve has lent to foreign central banks, and the Eurosystem to its own banks. These rise in a dollar shortage.
- `21:49:06`   FED_SWAPS        Federal Reserve central bank liquidity swaps outstanding
- `21:49:06`       0.146 usd_bn on 2026-11-18 (current, 4 days old) | 987 obs since 2007-12-26 | high [583.135, '2008-12-17'], low [0.0, '2008-03-05'] | 1w -0.025, 1m -0.059, 12m 0.115 | at or below 40.5% of its history | fred:SWPT
- `21:49:06`       highest separate readings: 583.135 on 2008-12-17 | 448.946 on 2020-05-27 | 282.863 on 2009-04-22 | 109.088 on 2012-02-15 | 63.287 on 2009-09-02
- `21:49:06`       inside windows: Lehman and the global crisis lo 62.0 hi 583.135 | Italy and Spain drawn in lo 0.0 hi 99.823 | COVID lo 0.044 hi 438.953 | SVB and Credit Suisse lo 0.411 hi 0.587 | US tariff shock lo 0.062 hi 0.103
- `21:49:06`   FED_FIMA         Federal Reserve repurchase agreements with foreign official accounts
- `21:49:06`       0.001 usd_bn on 2026-11-18 (current, 4 days old) | 346 obs since 2020-04-08 | high [60.0, '2023-03-22'], low [0.0, '2020-04-15'] | 1w 0.001, 1m 0.001, 12m 0.001 | at or below 84.4% of its history | fred:H41RESPPALGTRFNWW
- `21:49:06`       highest separate readings: 60.0 on 2023-03-22 | 3.0 on 2026-02-04 | 1.404 on 2020-05-13 | 1.0 on 2020-09-30 | 1.0 on 2021-02-03
- `21:49:06`       inside windows: COVID lo 0.0 hi 0.002 | SVB and Credit Suisse lo 0.0 hi 60.0 | US tariff shock lo 0.0 hi 1.0
- `21:49:06`   AUX_FX_EA        Claims on euro-area residents in foreign currency
- `21:49:06`       20.193 eur_bn on 2026-11-13 (current, 9 days old) | 1455 obs since 1999-01-01 | high [248.854, '2008-12-05'], low [5.255, '1999-01-08'] | 1w 0.411, 1m -1.114, 12m -2.102 | at or below 32.8% of its history | ILM.W.U2.C.A030000.U2.Z06
- `21:49:06`       highest separate readings: 248.854 on 2008-12-05 | 153.774 on 2020-05-08 | 142.13 on 2009-04-10 | 100.629 on 2012-02-10 | 61.288 on 2009-08-14
- `21:49:06`       inside windows: Lehman and the global crisis lo 85.5 hi 248.854 | Italy and Spain drawn in lo 23.204 hi 98.226 | COVID lo 22.805 hi 150.104 | SVB and Credit Suisse lo 14.04 hi 15.438 | US tariff shock lo 18.377 hi 19.599
- `21:49:06` The price of the dollar - A dollar shortage bids the dollar up against other currencies.
- `21:49:06`   USD_DXY          US dollar index (DXY), rebuilt from the Federal Reserve's H.10 rates
- `21:49:06`       103.3578 idx on 2026-11-13 (current, 9 days old) | 6988 obs since 1999-01-04 | high [120.8553, '2001-07-05'], low [71.2132, '2008-04-22'] | 1w 1.2083, 1m 1.1406, 12m 4.3215 | at or below 82.0% of its history | fred:DEXUSEU, fred:DEXJPUS, fred:DEXUSUK, fred:DEXCAUS, fred:DEXSDUS, fred:DEXSZUS
- `21:49:06`       highest separate readings: 120.8553 on 2001-07-05 | 120.1484 on 2002-01-28 | 118.6779 on 2000-10-26 | 113.9657 on 2022-09-26 | 112.9786 on 2001-02-26
- `21:49:06`       inside windows: Lehman and the global crisis lo 76.4607 hi 89.1522 | Italy and Spain drawn in lo 73.6097 hi 80.6045 | COVID lo 95.1068 hi 102.4305 | SVB and Credit Suisse lo 102.1943 hi 105.6227 | US tariff shock lo 98.355 hi 103.7262
- `21:49:06`   USD_BROAD        Nominal broad US dollar index
- `21:49:06`       122.6503 idx on 2026-11-13 (current, 9 days old) | 5250 obs since 2006-01-02 | high [130.0413, '2025-01-13'], low [85.4692, '2011-07-26'] | 1w 0.77, 1m 1.207, 12m 1.8778 | at or below 93.4% of its history | fred:DTWEXBGS
- `21:49:06`       highest separate readings: 130.0413 on 2025-01-13 | 128.4539 on 2022-09-26 | 126.1262 on 2020-03-23 | 124.4667 on 2024-07-01 | 124.0064 on 2023-10-26
- `21:49:06`       inside windows: Lehman and the global crisis lo 90.1119 hi 106.0249 | Italy and Spain drawn in lo 85.4692 hi 93.2176 | COVID lo 116.4958 hi 126.1262 | SVB and Credit Suisse lo 119.3602 hi 122.1079 | US tariff shock lo 122.3145 hi 126.8668
- `21:49:06`   USD_AFE          Nominal US dollar index against advanced foreign economies
- `21:49:06`       116.236 idx on 2026-11-13 (current, 9 days old) | 5231 obs since 2006-01-02 | high [124.8505, '2022-09-26'], low [81.3834, '2011-05-02'] | 1w 1.3718, 1m 1.1381, 12m 3.9757 | at or below 92.6% of its history | fred:DTWEXAFEGS
- `21:49:06`       highest separate readings: 124.8505 on 2022-09-26 | 122.5524 on 2025-01-13 | 119.0811 on 2023-10-03 | 118.4156 on 2024-04-16 | 117.8693 on 2023-03-08
- `21:49:06`       inside windows: Lehman and the global crisis lo 87.6647 hi 103.3224 | Italy and Spain drawn in lo 81.5021 hi 88.9963 | COVID lo 108.4606 hi 116.9532 | SVB and Credit Suisse lo 114.4897 hi 117.8693 | US tariff shock lo 111.4244 hi 117.1123
- `21:49:06`   USD_EME          Nominal US dollar index against emerging market economies
- `21:49:06`       130.8877 idx on 2026-11-13 (current, 9 days old) | 5231 obs since 2006-01-02 | high [139.9892, '2025-04-09'], low [89.1519, '2008-07-25'] | 1w 0.1164, 1m 1.2942, 12m -0.4416 | at or below 90.3% of its history | fred:DTWEXEMEGS
- `21:49:06`       highest separate readings: 139.9892 on 2025-04-09 | 137.7008 on 2024-11-26 | 137.2907 on 2020-03-23 | 134.5698 on 2022-10-24 | 133.3772 on 2024-07-29
- `21:49:06`       inside windows: Lehman and the global crisis lo 93.0406 hi 110.1208 | Italy and Spain drawn in lo 90.7078 hi 99.3254 | COVID lo 124.4247 hi 137.2907 | SVB and Credit Suisse lo 125.7662 hi 128.6228 | US tariff shock lo 135.0816 hi 139.9892
- `21:49:06`   AUX_EURUSD       EUR/USD reference rate
- `21:49:06`       1.099 usd on 2026-11-20 (current, 2 days old) | 7141 obs since 1999-01-04 | high [1.599, '2008-07-15'], low [0.8252, '2000-10-26'] | 1w -0.0062, 1m -0.0225, 12m -0.0524 | at or below 29.8% of its history | EXR.D.USD.EUR.SP00.A
- `21:49:06`       highest separate readings: 1.599 on 2008-07-15 | 1.577 on 2008-03-17 | 1.512 on 2009-12-03 | 1.4882 on 2011-05-04 | 1.4722 on 2007-11-07
- `21:49:06`       inside windows: Lehman and the global crisis lo 1.246 hi 1.4731 | Italy and Spain drawn in lo 1.2889 hi 1.45 | COVID lo 1.0707 hi 1.1456 | SVB and Credit Suisse lo 1.0545 hi 1.0886 | US tariff shock lo 1.0803 hi 1.1476
- `21:49:06` Foreign official dollars at the Federal Reserve - What foreign central banks keep at the Federal Reserve. Securities in custody fall when they sell or pledge them to raise dollars.
- `21:49:06`   FED_CUSTODY_UST  US Treasury securities held in custody at the Federal Reserve for foreign official and international accounts
- `21:49:06`       2544.011 usd_bn on 2026-11-18 (current, 4 days old) | 1012 obs since 2007-07-04 | high [3135.486, '2021-03-10'], low [1171.809, '2007-09-12'] | 1w 19.822, 1m -6.878, 12m -201.396 | at or below 17.6% of its history | fred:WMTSECL1
- `21:49:06`       highest separate readings: 3135.486 on 2021-03-10 | 3108.76 on 2018-03-14 | 3098.206 on 2021-07-14 | 3080.163 on 2019-03-20 | 3064.763 on 2018-08-08
- `21:49:06`       inside windows: Lehman and the global crisis lo 1407.462 hi 1722.183 | Italy and Spain drawn in lo 2599.14 hi 2675.692 | COVID lo 2843.669 hi 3004.655 | SVB and Credit Suisse lo 2863.419 hi 2950.306 | US tariff shock lo 2916.875 hi 2943.493
- `21:49:06`   FED_CUSTODY_ALL  Securities held in custody at the Federal Reserve for foreign official and international accounts
- `21:49:06`       2813.231 usd_bn on 2026-11-18 (current, 4 days old) | 1012 obs since 2007-07-04 | high [3577.416, '2021-03-10'], low [1904.167, '2007-09-12'] | 1w 19.618, 1m -9.063, 12m -247.686 | at or below 15.9% of its history | fred:WSEFINTL1
- `21:49:06`       highest separate readings: 3577.416 on 2021-03-10 | 3542.632 on 2021-07-14 | 3476.205 on 2019-03-20 | 3473.619 on 2021-11-17 | 3472.3 on 2019-08-14
- `21:49:06`       inside windows: Lehman and the global crisis lo 2283.94 hi 2424.238 | Italy and Spain drawn in lo 3038.758 hi 3140.736 | COVID lo 3314.71 hi 3463.805 | SVB and Credit Suisse lo 3284.057 hi 3360.421 | US tariff shock lo 3271.528 hi 3301.241
- `21:49:06`   FED_FOREIGN_RRP  Federal Reserve reverse repurchase agreements with foreign official and international accounts
- `21:49:06`       316.712 usd_bn on 2026-11-18 (current, 4 days old) | 1249 obs since 2002-12-18 | high [420.009, '2024-09-11'], low [17.18, '2003-03-19'] | 1w -19.389, 1m -9.886, 12m -19.756 | at or below 84.0% of its history | fred:WLRRAFOIAL
- `21:49:06`       highest separate readings: 420.009 on 2024-09-11 | 393.831 on 2025-07-02 | 392.468 on 2025-01-22 | 385.029 on 2023-05-10 | 380.593 on 2022-11-30
- `21:49:06`       inside windows: Lehman and the global crisis lo 46.633 hi 88.913 | Italy and Spain drawn in lo 64.695 hi 124.512 | COVID lo 217.668 hi 286.599 | SVB and Credit Suisse lo 367.113 hi 370.376 | US tariff shock lo 357.656 hi 388.765
- `21:49:06` Banks' dollar funding - What dollars cost overnight against what the Federal Reserve pays on reserves, and what foreign banks' US offices owe their offices abroad. The TED spread ended in 2022 and is kept for its history.
- `21:49:06`   USD_SOFR_IOR     SOFR minus the interest rate the Federal Reserve pays on reserves
- `21:49:06`       1.0 bp on 2026-11-19 (current, 3 days old) | 2156 obs since 2018-04-03 | high [315.0, '2019-09-17'], low [-21.0, '2022-06-14'] | 1w 1.0, 1m 4.0, 12m 0.0 | at or below 85.4% of its history | fred:SOFR, fred:IORB, fred:IOER
- `21:49:06`       highest separate readings: 315.0 on 2019-09-17 | 75.0 on 2019-01-02 | 44.0 on 2020-03-17 | 32.0 on 2025-10-31 | 17.0 on 2018-06-29
- `21:49:06`       inside windows: COVID lo -9.0 hi 44.0 | SVB and Credit Suisse lo -10.0 hi -3.0 | US tariff shock lo -12.0 hi 2.0
- `21:49:06`   USD_TED          TED spread: three-month dollar LIBOR minus the three-month Treasury bill
- `21:49:06`       9.0 bp on 2022-01-21 (ended, 1766 days old) | 5656 obs since 1999-01-04 | high [458.0, '2008-10-10'], low [6.0, '2021-07-08'] | 1w -2.0, 1m -6.0, 12m -4.0 | at or below 1.8% of its history | fred:TEDRATE
- `21:49:06`       highest separate readings: 458.0 on 2008-10-10 | 242.0 on 2007-08-20 | 206.0 on 2007-12-19 | 168.0 on 2008-04-23 | 150.0 on 1999-10-08
- `21:49:06`       inside windows: Lehman and the global crisis lo 91.0 hi 458.0 | Italy and Spain drawn in lo 16.0 hi 57.0 | COVID lo 11.0 hi 142.0
- `21:49:06`   H8_NET_DUE       Net due to related foreign offices, foreign-related institutions in the United States
- `21:49:06`       885.647 usd_bn on 2026-11-11 (current, 11 days old) | 1454 obs since 1999-01-06 | high [1007.565, '2025-07-23'], low [-615.423, '2008-05-28'] | 1w 9.609, 1m 49.07, 12m 103.047 | at or below 98.1% of its history | fred:NDFFRIW027NBOG
- `21:49:06`       highest separate readings: 1007.565 on 2025-07-23 | 937.085 on 2025-03-19 | 923.903 on 2026-04-08 | 885.647 on 2026-11-11 | 837.771 on 2025-11-26
- `21:49:06`       inside windows: Lehman and the global crisis lo -590.194 hi -81.772 | Italy and Spain drawn in lo 50.355 hi 300.067 | COVID lo 123.528 hi 534.248 | SVB and Credit Suisse lo 666.073 hi 764.734 | US tariff shock lo 871.164 hi 955.175
- `21:49:06` The Eurosystem's own foreign reserves - Shown because it sits beside the lending item on the same statement; it is not a gauge of a dollar shortage.
- `21:49:06`   AUX_FX_NONEA     Claims on non-euro-area residents in foreign currency
- `21:49:06`       521.892 eur_bn on 2026-11-13 (current, 9 days old) | 1455 obs since 1999-01-01 | high [552.989, '2022-11-04'], low [131.15, '2008-09-19'] | 1w -1.6, 1m -2.574, 12m 27.316 | at or below 96.7% of its history | ILM.W.U2.C.A020000.U4.Z06
- `21:49:06`       highest separate readings: 552.989 on 2022-11-04 | 526.308 on 2026-08-28 | 524.342 on 2025-01-24 | 519.276 on 2022-07-01 | 517.721 on 2026-04-10
- `21:49:06`       inside windows: Lehman and the global crisis lo 131.15 hi 163.037 | Italy and Spain drawn in lo 213.018 hi 244.621 | COVID lo 345.772 hi 360.356 | SVB and Credit Suisse lo 501.658 hi 508.684 | US tariff shock lo 510.056 hi 515.373
- `21:49:06`   as the record has it: swap lines peaked above $500 bn in December 2008
- `21:49:06`   as the record has it: swap lines were drawn above $300 bn inside the COVID window
- `21:49:06`   as the record has it: repos with foreign central banks peaked in March 2023
- `21:49:06`   as the record has it: the TED spread peaked at 458 bp on 10 October 2008 and ended on 21 January 2022
- `21:49:06`   as the record has it: SOFR stood 315 bp over the rate on reserves on 17 September 2019
- `21:49:06`   as the record has it: foreign banks' US offices were owed $615 bn by their offices abroad in May 2008
- `21:49:06`   as the record has it: DXY's low was near 71 in 2008 and its high near 120 in 2001-02
- `21:49:06`   as the record has it: the broad dollar index begins on 2 January 2006
- `21:49:06`   as the record has it: Treasuries in custody are above $2,000 bn and the line has no reading below $1,000 bn since it began in 2007
- `21:49:06`   as the record has it: all securities in custody: no reading below $1,500 bn since the line began in 2007
- `21:49:06`   as the record has it: the repo line with foreign central banks begins in 2020
- `21:49:06`   as the record has it: the swap line begins with its first drawing in December 2007
- `21:49:06` not current: [('USD_TED', 'ended', '2022-01-21')]
- `21:49:06` swap lines through March-May 2020: 2020-03-04 0.048 | 2020-03-11 0.058 | 2020-03-18 0.045 | 2020-03-25 206.051 | 2020-04-01 348.544 | 2020-04-08 358.077 | 2020-04-15 378.291 | 2020-04-22 409.712 | 2020-04-29 438.953 | 2020-05-06 444.885 | 2020-05-13 440.934 | 2020-05-20 446.103 | 2020-05-27 448.946 | 2020-06-03 446.945
- `21:49:06` March 2020, Treasuries in custody and the foreign repo pool: 2020-02-26 3004.655 / 217.668 | 2020-03-04 2982.812 / 231.677 | 2020-03-11 2961.623 / 231.95 | 2020-03-18 2919.968 / 233.469 | 2020-03-25 2868.19 / 261.703 | 2020-04-01 2855.182 / 286.599 | 2020-04-08 2843.669 / 281.005 | 2020-04-15 2857.372 / 276.434 | 2020-04-22 2853.978 / 278.467 | 2020-04-29 2871.607 / 267.656
## 4. A second run in the same day asks for nothing

- `21:49:07` receipt: series_asked 0, measures 15, wrote ['data/ciss-dollar-shortage.json']
- `21:49:07` ✅ the dollar-shortage document compiles with 3 ECB and 12 Federal Reserve measures; nothing was written
