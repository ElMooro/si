# ops 5190 -- TradingView refusal + fallbacks

**Status:** success  
**Duration:** 11.2s  
**Finished:** 2026-09-04T13:28:11+00:00  

## Log
## A. tv-bars engine recent log lines

- `13:28:01`    (no matching log lines in 48h)
## B. TradingView scanner REST (no auth)

- `13:28:01`    https://scanner.tradingview.com/global/scan -> 200 rows=26 sample=[('TVC:US10Y', [4.78, 0.16764459346186097, 0.008000000000000007]), ('TVC:DE10Y', [3.3428, -0.3131243849342425, -0.010499999999999954]), ('TVC:IT10Y', [4.1574, -0.4310964218996933, -0.017999999999999794]), ('TVC:ES10Y', [3.7758, -0.5635731591699228, -0.021400000000000308]), ('TVC:FR10Y', [4.1926, -0.5927541729893863, -0.025000000000000355]), ('TVC:GB10Y', [5.1435, -0.10487676979548738, -0.005399999999999849])]
- `13:28:01`    https://scanner.tradingview.com/bonds/scan -> 200 rows=25 sample=[('TVC:US10Y', [4.78, 0.16764459346186097, 0.008000000000000007]), ('TVC:DE10Y', [3.3428, -0.3131243849342425, -0.010499999999999954]), ('TVC:IT10Y', [4.1574, -0.4310964218996933, -0.017999999999999794]), ('TVC:ES10Y', [3.7758, -0.5635731591699228, -0.021400000000000308]), ('TVC:FR10Y', [4.1926, -0.5927541729893863, -0.025000000000000355]), ('TVC:GB10Y', [5.1435, -0.10487676979548738, -0.005399999999999849])]
- `13:28:01`    https://scanner.tradingview.com/america/scan -> 200 rows=0 sample=[]
## C. MOF JGB history CSV

- `13:28:03`    olicy/jgbs/reference/interest_rate/historical/jgbcme_all.csv -> 200 bytes=1193668 tail=b',4.096,4.084,4.084\r\n2026/8/31,1.502,1.743,1.894,2.084,2.233,2.358,2.507,2.67,2.801,2.943,3.501,3.815,4.102,4.092,4.094\r\n'
- `13:28:04`    ww.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv -> 200 bytes=1176385 tail=b'06,4.096,4.084,4.084\r\nR8.8.31,1.502,1.743,1.894,2.084,2.233,2.358,2.507,2.67,2.801,2.943,3.501,3.815,4.102,4.092,4.094\r\n'
- `13:28:04`    p/english/policy/jgbs/reference/interest_rate/jgbcme_all.csv -> 404 bytes=300 tail=b'\xef\xbb\xbf<!DOCTYPE html>\n<html lang="ja">\n\n<head prefix="og: http://ogp.me/ns# fb: htt'
## D. Bundesbank + ECB daily

- `13:28:05`    BBSSY/D.REN.EUR.A630.000000WT1010.A?lastNObservations=5&form -> 200 ﻿"";BBSSY.D.REN.EUR.A630.000000WT1010.A;BBSSY.D.REN.EUR.A630.000000WT1010.A_FLAGS | "";Rendite der jeweils jüngsten Bundesanleihe mit einer vereinbarten Laufzeit von 10 Jahren; | Dezimalstellen;2; | Dime
- `13:28:06`    BBSSY/D.REN.EUR.A630.000000WT1010.A?lastNObservations=5&form -> 200 {"meta":{"schema":"https://raw.githubusercontent.com/sdmx-twg/sdmx-json/develop/data-message/tools/schemas/1.0/sdmx-json-data-schema.json","id":"BBSSY_D_REN_EUR_A630_000000WT1010_A","test":false,"prep
- `13:28:07`    YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y?lastNObservations=3&format=csvdata -> 200 KEY,FREQ,REF_AREA,CURRENCY,PROVIDER_FM,INSTRUMENT_FM,PROVIDER_FM_ID,DATA_TYPE_FM,TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK,OBS_COM,TIME_FORMAT,BREAKS,COLLECTION,COMPILING_ORG,DISS_ORG,DO
- `13:28:08`    YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y?lastNObservations=3&format=csvdata -> 200 KEY,FREQ,REF_AREA,CURRENCY,PROVIDER_FM,INSTRUMENT_FM,PROVIDER_FM_ID,DATA_TYPE_FM,TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK,OBS_COM,TIME_FORMAT,BREAKS,COLLECTION,COMPILING_ORG,DISS_ORG,DO
- `13:28:09`    FM/B.U2.EUR.4F.BB.U2_10Y.YLD?lastNObservations=3&format=csvdata -> 404 {"type":"/service/data/FM/B.U2.EUR.4F.BB.U2_10Y.YLD","title":"Not Found","status":404,"detail":"No Series was returned for the query: <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Data
- `13:28:10`    FM/B.IT.EUR.4F.BB.IT_10Y.YLD?lastNObservations=3&format=csvdata -> 404 {"type":"/service/data/FM/B.IT.EUR.4F.BB.IT_10Y.YLD","title":"Not Found","status":404,"detail":"No Series was returned for the query: <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Data
- `13:28:11`    IRS/M.IT.L.L40.CI.0000.EUR.N.Z?lastNObservations=2&format=csvdata -> 200 KEY,FREQ,REF_AREA,IR_TYPE,TR_TYPE,MATURITY_CAT,BS_COUNT_SECTOR,CURRENCY_TRANS,IR_BUS_COV,IR_FV_TYPE,TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK,OBS_COM,TIME_FORMAT,BREAKS,COLLECTION,COMPIL
- `13:28:11` ✅ probe complete
