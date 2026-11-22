# ops 6926 - the dollar-shortage document with the Federal Reserve's series, first compile by the deployed engine, 2026-11-22T22:08+00:00

**Status:** success  
**Duration:** 18.6s  
**Finished:** 2026-11-22T22:08:40+00:00  

## Data

| bytes | failed_checks | measures | observations | seconds |
|---|---|---|---|---|
| 1020303 | 0 | 15 | 46610 | 15.2 |

## Log
## 1. Guard

- `22:08:22` justhodl-ciss-stress updated 2026-11-22T22:07:26 | as in this checkout: {'dollar_shortage.py': True, 'world_history.py': True, 'sovereign_desk.py': True, 'lambda_function.py': True}
- `22:08:23` before: data/ciss-dollar-shortage.json version 1.0.0, 3 measures, generated 2026-11-22T19:12:16.504909+00:00
- `22:08:23` before: no series are kept yet (data/ciss-research/sovereign/dollar-state.json)
## 2. One invoke, the dollar-shortage document only

- `22:08:38` invoke: HTTP 200, function error None, 15.2s | {"dollar": {"published": true, "generated_at": "2026-11-22T22:08:24.557320+00:00", "desk_generated_at": "2026-11-22T16:39:59.412623+00:00", "coverage": {"measures": 15, "observations": 46610, "ecb": 3, "fed": 12, "errors": 0, "status": "measured"}, "series_asked": 18, "latest": {"FED_SWAPS": [0.146, "2026-11-18"], "FED_FIMA": [0.001, "2026-11-18"], "AUX_FX_EA": [20.193, "2026-11-13"], "USD_DXY": [103.3578, "2026-11-13"], "USD_BROAD": [122.6503, "2026-11-13"], "USD_AFE": [116.236, "2026-11-13"], "USD_EME": [130.8877, "2026-11-13"], "AUX_EURUSD": [1.099, "2026-11-20"], "FED_CUSTODY_13W": [-42.16
## 3. Read back

- `22:08:39` document 1020303 bytes written 22:08:39 | contract ciss-dollar-shortage.v1 1.1.0 | generated 2026-11-22T22:08:24.557320+00:00 | stands on the desk document of 2026-11-22T16:39
- `22:08:39` Dollars lent by central banks
- `22:08:39`   FED_SWAPS        0.146 usd_bn on 2026-11-18 (current) | 987 observations 2007-12-26 .. 2026-11-18 | high [583.135, '2008-12-17'] | low [0.0, '2008-03-05'] | Fed swap lines drawn
- `22:08:39`   FED_FIMA         0.001 usd_bn on 2026-11-18 (current) | 346 observations 2020-04-08 .. 2026-11-18 | high [60.0, '2023-03-22'] | low [0.0, '2020-04-15'] | Fed repos with foreign central banks (FIMA)
- `22:08:39`   AUX_FX_EA        20.193 eur_bn on 2026-11-13 (current) | 1455 observations 1999-01-01 .. 2026-11-13 | high [248.854, '2008-12-05'] | low [5.255, '1999-01-08'] | Eurosystem foreign-currency lending to euro-area banks
- `22:08:39` The price of the dollar
- `22:08:39`   USD_DXY          103.3578 idx on 2026-11-13 (current) | 6988 observations 1999-01-04 .. 2026-11-13 | high [120.8553, '2001-07-05'] | low [71.2132, '2008-04-22'] | DXY, rebuilt from the Fed's H.10 rates
- `22:08:39`   USD_BROAD        122.6503 idx on 2026-11-13 (current) | 5250 observations 2006-01-02 .. 2026-11-13 | high [130.0413, '2025-01-13'] | low [85.4692, '2011-07-26'] | Broad dollar index
- `22:08:39`   USD_AFE          116.236 idx on 2026-11-13 (current) | 5231 observations 2006-01-02 .. 2026-11-13 | high [124.8505, '2022-09-26'] | low [81.3834, '2011-05-02'] | Dollar against advanced economies
- `22:08:39`   USD_EME          130.8877 idx on 2026-11-13 (current) | 5231 observations 2006-01-02 .. 2026-11-13 | high [139.9892, '2025-04-09'] | low [89.1519, '2008-07-25'] | Dollar against emerging markets
- `22:08:39`   AUX_EURUSD       1.099 usd on 2026-11-20 (current) | 7141 observations 1999-01-04 .. 2026-11-20 | high [1.599, '2008-07-15'] | low [0.8252, '2000-10-26'] | EUR/USD
- `22:08:39` Foreign official dollars at the Federal Reserve
- `22:08:39`   FED_CUSTODY_13W  -42.16 usd_bn on 2026-11-18 (current) | 999 observations 2007-10-03 .. 2026-11-18 | high [271.721, '2010-11-03'] | low [-163.729, '2014-03-12'] | Treasuries in custody: 13-week change
- `22:08:39`   FED_CUSTODY_UST  2544.011 usd_bn on 2026-11-18 (current) | 1012 observations 2007-07-04 .. 2026-11-18 | high [3135.486, '2021-03-10'] | low [1171.809, '2007-09-12'] | Treasuries in custody for foreign central banks
- `22:08:39`   FED_FOREIGN_RRP  316.712 usd_bn on 2026-11-18 (current) | 1249 observations 2002-12-18 .. 2026-11-18 | high [420.009, '2024-09-11'] | low [17.18, '2003-03-19'] | Foreign repo pool at the Fed
- `22:08:39` Banks' dollar funding
- `22:08:39`   USD_SOFR_IOR     1.0 bp on 2026-11-19 (current) | 2156 observations 2018-04-03 .. 2026-11-19 | high [315.0, '2019-09-17'] | low [-21.0, '2022-06-14'] | SOFR minus interest on reserves
- `22:08:39`   USD_TED          9.0 bp on 2022-01-21 (ended) | 5656 observations 1999-01-04 .. 2022-01-21 | high [458.0, '2008-10-10'] | low [6.0, '2021-07-08'] | TED spread (ended 2022)
- `22:08:39`   H8_NET_DUE       885.647 usd_bn on 2026-11-11 (current) | 1454 observations 1999-01-06 .. 2026-11-11 | high [1007.565, '2025-07-23'] | low [-615.423, '2008-05-28'] | Foreign banks' US offices: net owed to offices abroad
- `22:08:39` The Eurosystem's own foreign reserves
- `22:08:39`   AUX_FX_NONEA     521.892 eur_bn on 2026-11-13 (current) | 1455 observations 1999-01-01 .. 2026-11-13 | high [552.989, '2022-11-04'] | low [131.15, '2008-09-19'] | Eurosystem foreign reserves other than gold
- `22:08:39` the ECB's three against the desk document and its weekly history: 0 problems []
- `22:08:39` the twelve Federal Reserve measures against the dry run of 2026-11-22T22:03: 0 differences []
- `22:08:39` kept series: 18, read at ['2026-11-22T22:08']
- `22:08:40` through the worker the Dollar desk reads: HTTP 200, allow-origin *, generated 2026-11-22T22:08:24.557320+00:00, same document: True
- `22:08:40` the warehouse, the desk document, its history and the world histories are as before: True
- `22:08:40` ✅ the deployed engine published the dollar-shortage document with 3 ECB and 12 Federal Reserve measures; the ECB's equal the desk document, the rest equal the dry run, and nothing else was touched
