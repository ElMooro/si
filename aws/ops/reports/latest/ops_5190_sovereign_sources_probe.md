# ops 5190 -- sovereign yield sources (no TradingView socket)

**Status:** success  
**Duration:** 26.3s  
**Finished:** 2026-09-04T13:29:44+00:00  

## Log
## A. Eurostat irt_lt_mcby_d (daily Maastricht 10Y yields) -- live API

- `13:29:18`    DE  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:18`    IT  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:19`    ES  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:19`    FR  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:20`    NL  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:20`    PT  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:21`    GR  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:21`    AT  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:22`    BE  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:22`    IE  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:22`    FI  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:23`    EA  -> 404 parse-fail 'dimension' body=b'{ "error": [{"status": 404,"id": 100,"label": "ERR_NOT_FOUND_4: IRT_LT_MCBY_D (DATA_FLOW:ALL,1.0) is not available for d'
- `13:29:23`    IT full history -> 404 fail 'dimension'
## A2. our Eurostat mirror

- `13:29:26`    scanned 8148 objects under data/warm/eurostat/; irt_lt_mcby / irt_euryld hits: []
## B. ECB Data Portal daily (YC euro-area curve, FM benchmarks)

- `13:29:27`    YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y           -> 200 rows=6 last=['SR_10Y', '2026-09-03']
- `13:29:28`    YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y           -> 200 rows=6 last=['SR_10Y', '2026-09-03']
- `13:29:29`    YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y            -> 200 rows=6 last=['SR_2Y', '2026-09-03']
- `13:29:30`    FM/B.U2.EUR.4F.BB.U2_10Y.YLD                  -> 404 rows=0 last=b'{"type":"/service/data/FM/B.U2.EUR.4F.BB.U2_10Y.YLD","title":"Not Found","status":404,"detail":"No S'
- `13:29:31`    FM/B.DE.EUR.4F.BB.DE10Y_RR.YLD                -> 404 rows=0 last=b'{"type":"/service/data/FM/B.DE.EUR.4F.BB.DE10Y_RR.YLD","title":"Not Found","status":404,"detail":"No'
- `13:29:32`    FM/B.IT.EUR.4F.BB.IT10Y_RR.YLD                -> 404 rows=0 last=b'{"type":"/service/data/FM/B.IT.EUR.4F.BB.IT10Y_RR.YLD","title":"Not Found","status":404,"detail":"No'
## C. Bundesbank BBSIS daily Bund yields

- `13:29:32`    2Y   R02XX -> 200 rows=13 tail=['2026-09-04;2', '94;']
- `13:29:33`    10Y  R10XX -> 200 rows=13 tail=['2026-09-04;3', '37;']
- `13:29:33`    30Y  R30XX -> 200 rows=13 tail=['2026-09-04;3', '80;']
## D. Bank of England gilt par yields (IUDSNPY 5y, IUDMNZC 10y, IUDLNPY 20y)

- `13:29:34`    -> 200 rows=677 head=[['DATE', 'IUDSNPY', 'IUDMNZC', 'IUDLNPY']] tail=[['02 Sep 2026', '4.6975', '5.2416', '5.6469']]
- `13:29:35`    alt -> 200 rows=1378 tail=[['</html>']]
## E. Bank of Canada Valet

- `13:29:35`    -> 200 obs=5 last={'d': '2026-08-28', 'BD.CDN.LONG.DQ.YLD': {'v': '4.14'}, 'BD.CDN.10YR.DQ.YLD': {'v': '3.73'}, 'BD.CDN.2YR.DQ.YLD': {'v': '3.01'}, 'BD.CDN.5YR.DQ.YLD': {'v': '3.34'}}
## F. RBA F2 daily government bond yields

- `13:29:36`    -> 200 rows=64778 header=[['Title', 'Australian Government 2 year bond', 'Australian Government 3 year bond', 'Australian Government 5 year bond', 'Australian Government 10 year bond', 'Australian Government Indexed Bond'], ['Series ID', 'FCMYGBAG2D', 'FCMYGBAG3D', 'FCMYGBAG5D', 'FCMYGBAG10D', 'FCMYGBAGID']] tail=[]
## G. Japan MOF JGB full history

- `13:29:38`    jgbcme_all.csv -> 200 lines=13292 head=['Interest Rate,,,,,,,,,,,,,,,(Unit : %)', 'Date,1Y,2Y,3Y,4Y,5Y,6Y,7Y,8Y,9Y,10Y,15Y,20Y,25Y,30Y,40Y'] tail=['2026/8/31,1.502,1.743,1.894,2.084,2.233,2.358,2.507,2.67,2.801,2.943,3.501,3.815,4.102,4.092,4.094']
## H. SNB rendoblid (CHF confederation yields)

- `13:29:41`    ta.snb.ch/api/cube/rendoblid/data/csv/en -> 200 bytes=5461632 head=b'\xef\xbb\xbf"CubeId";"rendoblid"\r\n"PublishingDate";"2025-09-01 14:29"\r\n\r\n"Date";"D0";"Value"\r\n"1988-01-01";"1J";\r\n"1988-01-01";"2J";\r\n"1988-01-01";"3J";\r\n"1988-01-01";"4'
- `13:29:42`    ndoblid/data/json/en?fromDate=2026-08-01 -> 200 bytes=6718 head=b'{"timeseries":[{"header":[{"dim":"Overview","dimItem":"Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond iss'
## I. Treasury daily par curve month XML + our bank

- `13:29:42`    month XML -> 200 bytes=5308 NEW_DATE count=6
- `13:29:43`    bank as_of=2026-09-03T21:31:09+00:00 n_days=9176 first=1990-01-02 last=2026-09-03 tenors=['1M', '2M', '3M', '4M', '6M', '1Y', '2Y', '3Y', '5Y', '7Y', '10Y', '20Y', '30Y']
## J. TradingView REST scanner (no socket)

- `13:29:43`    global -> 200 n=23 sample=[('TVC:US10Y', [4.778, 0.1257334450963818, 0.005999999999999339, 'streaming']), ('TVC:US02Y', [4.379, 0.8986175115207304, 0.0389999999999997, 'streaming']), ('TVC:DE10Y', [3.3428, -0.3131243849342425, -0.010499999999999954, 'streaming']), ('TVC:DE02Y', [2.9446, -0.6947254822608914, -0.02059999999999995, 'streaming']), ('TVC:IT10Y', [4.156, -0.46462614360301013, -0.019400000000000084, 'streaming']), ('TVC:IT02Y', [3.1414, -0.9584463080900499, -0.030400000000000205, 'streaming'])]
- `13:29:43`    bonds -> 200 n=22 sample=[('TVC:US10Y', [4.778, 0.1257334450963818, 0.005999999999999339, 'streaming']), ('TVC:US02Y', [4.379, 0.8986175115207304, 0.0389999999999997, 'streaming']), ('TVC:DE10Y', [3.3428, -0.3131243849342425, -0.010499999999999954, 'streaming']), ('TVC:DE02Y', [2.9446, -0.6947254822608914, -0.02059999999999995, 'streaming']), ('TVC:IT10Y', [4.156, -0.46462614360301013, -0.019400000000000084, 'streaming']), ('TVC:IT02Y', [3.1414, -0.9584463080900499, -0.030400000000000205, 'streaming'])]
- `13:29:43`    america -> 200 n=0 sample=[]
## K. Yahoo US yields via worker

- `13:29:43`    ^IRX  -> 200 bars=24 last={'time': 1788524400, 'close': 3.7699999809265137}
- `13:29:43`    ^FVX  -> 200 bars=24 last={'time': 1788524400, 'close': 4.556000232696533}
- `13:29:43`    ^TNX  -> 200 bars=24 last={'time': 1788524400, 'close': 4.783999919891357}
- `13:29:44`    ^TYX  -> 200 bars=24 last={'time': 1788524400, 'close': 5.245999813079834}
- `13:29:44` ✅ probe complete
