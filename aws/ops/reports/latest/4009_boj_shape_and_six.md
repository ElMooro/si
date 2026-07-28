# ops 4009 — BOJ shape + FI/ES/IT/CH/TW/KR/BR discovery

**Status:** failure  
**Duration:** 596.6s  
**Finished:** 2026-07-28T04:55:57+00:00  

## Error

```
SystemExit: 1
```

## Data

| n_confirmed | n_deferred |
|---|---|
| 3 | 13 |

## Log
## V. collision check — candidate symbols in the live vault

- `04:46:01`   JPM3      EXISTS status=LIVE src=fred_alias:MABMM30
- `04:46:01`   CHINTR    EXISTS status=NO_FREE_SOURCE src=unresolved_economi
- `04:46:01`   IT10Y     EXISTS status=LIVE src=fred_alias:IRLTLT0
- `04:46:01`   ES10Y     EXISTS status=NO_FREE_SOURCE src=unresolved_tv_only
- `04:46:01`   TWINTR    EXISTS status=NO_FREE_SOURCE src=unresolved_economi
## A. BOJ getDataCode RAW — the shape work

- `04:46:02`   MD01/MABS1AN11 [200] bytes=739
- `04:46:02`   HEAD: { "STATUS":200, "MESSAGEID":"M181000I", "MESSAGE":"Successfully completed", "DATE":"2026-07-28T13:46:01.898+09:00", "PARAMETER":{ "FORMAT":"JSON", "LANG":"EN", "DB":"MD01", "STARTDATE":"202505", "ENDDATE":"202607", "STARTPOSITION":"" }, "NEXTPOSITION":null, "RESULTSET":[ { "SERIES_CODE":"MABS1AN11", "NAME_OF_TIME_SERIES":"Monetary Base/Average Amounts Outstanding", "UNIT":"100 million yen", "FREQU
- `04:46:02`   TAIL: ase", "LAST_UPDATE":20260702, "VALUES":{ "SURVEY_DATES":[202505,202506,202507,202508,202509,202510,202511,202512,202601,202602,202603,202604,202605,202606,202607], "VALUES":[6560120,6479525,6438964,6437775,6281080,6165980,6126869,5941943,5894035,5809326,5707875,5829256,5757634,5592039,null] } } ] } 
- `04:46:02`     .STATUS: int 200
- `04:46:02`     .MESSAGEID: str M181000I
- `04:46:02`     .MESSAGE: str Successfully completed
- `04:46:02`     .DATE: str 2026-07-28T13:46:01.898+09:00
- `04:46:02`     .PARAMETER: dict n=6
- `04:46:02`     .PARAMETER.FORMAT: str JSON
- `04:46:02`     .PARAMETER.LANG: str EN
- `04:46:02`     .PARAMETER.DB: str MD01
- `04:46:02`     .PARAMETER.STARTDATE: str 202505
- `04:46:02`     .PARAMETER.ENDDATE: str 202607
- `04:46:02`     .PARAMETER.STARTPOSITION: str 
- `04:46:02`     .NEXTPOSITION: NoneType None
- `04:46:02`     .RESULTSET: list n=1
- `04:46:02`     .RESULTSET[0]: {'SERIES_CODE': 'MABS1AN11', 'NAME_OF_TIME_SERIES': 'Monetary Base/Average Amounts Outstanding', 'UNIT': '100 million ye
- `04:46:02`     .RESULTSET[0].SERIES_CODE: str MABS1AN11
- `04:46:02`     .RESULTSET[0].NAME_OF_TIME_SERIES: str Monetary Base/Average Amounts Outstanding
- `04:46:02`     .RESULTSET[0].UNIT: str 100 million yen
- `04:46:02`     .RESULTSET[0].FREQUENCY: str MONTHLY
- `04:46:02`     .RESULTSET[0].CATEGORY: str Monetary Base
- `04:46:02`     .RESULTSET[0].LAST_UPDATE: int 20260702
- `04:46:02`     .RESULTSET[0].VALUES: dict n=2
- `04:46:02`     .RESULTSET[0].VALUES.SURVEY_DATES: list n=15
- `04:46:02`     .RESULTSET[0].VALUES.SURVEY_DATES[0]: 202505
- `04:46:02`     .RESULTSET[0].VALUES.VALUES: list n=15
- `04:46:02`     .RESULTSET[0].VALUES.VALUES[0]: 6560120
## A2. BOJ Tankan + call-rate code discovery

- `04:46:07`   [200] CO: pairs=166514 hits=5
- `04:46:07`       TK99F0000601GCQ00000 — D.I./Business Conditions/All Enterprises/All industries/Actual result
- `04:46:07`       TK99F0000601GCQ10000 — D.I./Business Conditions/All Enterprises/All industries/Forecast
- `04:46:07`       TK99F1000601GCQ00000 — D.I./Business Conditions/All Enterprises/Manufacturing/Actual result
- `04:46:07`       TK99F1000601GCQ10000 — D.I./Business Conditions/All Enterprises/Manufacturing/Forecast
- `04:46:07`   [200] FM08: pairs=59 hits=0
- `04:46:07`   [200] FM01: pairs=5 hits=3
- `04:46:07`       STRDCLUCON — Call Rate, Uncollateralized Overnight, Average (Daily)
- `04:46:07`       STRDCLUCONH — Call Rate, Uncollateralized Overnight, Highest (Daily)
- `04:46:07`       STRDCLUCONL — Call Rate, Uncollateralized Overnight, Lowest (Daily)
- `04:46:08`   [200] IR01: pairs=3 hits=0
## B. FRED MEI — keyless fredgraph confirms

- `04:46:53`   FIIPYY  FINPROINDMISMEI      [0] n=0 last=None
- `04:47:38`   ESIPYY  ESPPROINDMISMEI      [0] n=0 last=None
- `04:48:24`   ITIPYY  ITAPROINDMISMEI      [0] n=0 last=None
- `04:49:09`   CHIPYY  CHEPROINDMISMEI      [0] n=0 last=None
- `04:49:54`   KRIPYY  KORPROINDMISMEI      [0] n=0 last=None
- `04:50:39`   BRIPYY  BRAPROINDMISMEI      [0] n=0 last=None
- `04:51:24`   IT10Y   IRLTLT01ITM156N      [0] n=0 last=None
- `04:52:09`   ES10Y   IRLTLT01ESM156N      [0] n=0 last=None
- `04:52:54`   FI10Y   IRLTLT01FIM156N      [0] n=0 last=None
- `04:53:39`   CH10Y   IRLTLT01CHM156N      [0] n=0 last=None
- `04:54:24`   KR10Y   IRLTLT01KRM156N      [0] n=0 last=None
- `04:55:09`   CHINTR  IRSTCI01CHM156N      [0] n=0 last=None
- `04:55:54`   KRINTR  INTDSRKRM193N        [0] n=0 last=None
## C. BCB Brazil — SGS open API

- `04:55:55`   BRINTR  sgs.432 [200] 05/08/2026 v=14.25
- `04:55:56`   BRFER   sgs.13621 [200] 24/07/2026 v=368899.0
- `04:55:57`   BRLUSD  sgs.1 [200] 27/07/2026 v=5.1005
## E. PASTE-READY + DEFERRED

- `04:55:57`   "BRINTR": "bcb:432",  # Selic target
- `04:55:57`   "BRFER": "bcb:13621",  # intl reserves USD mn
- `04:55:57`   "BRLUSD": "bcb:1",  # USD/BRL
- `04:55:57`   DEFER FIIPYY: st=0 n=0
- `04:55:57`   DEFER ESIPYY: st=0 n=0
- `04:55:57`   DEFER ITIPYY: st=0 n=0
- `04:55:57`   DEFER CHIPYY: st=0 n=0
- `04:55:57`   DEFER KRIPYY: st=0 n=0
- `04:55:57`   DEFER BRIPYY: st=0 n=0
- `04:55:57`   DEFER IT10Y: st=0 n=0
- `04:55:57`   DEFER ES10Y: st=0 n=0
- `04:55:57`   DEFER FI10Y: st=0 n=0
- `04:55:57`   DEFER CH10Y: st=0 n=0
- `04:55:57`   DEFER KR10Y: st=0 n=0
- `04:55:57`   DEFER CHINTR: st=0 n=0
- `04:55:57`   DEFER KRINTR: st=0 n=0
- `04:55:57` ✗ under 8 — read the evidence before wiring
