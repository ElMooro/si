# ops 4111 — discovery round 2

**Status:** success  
**Duration:** 41.1s  
**Finished:** 2026-07-30T02:35:00+00:00  

## Data

| bis_areas | bytes | irfcl_areas | irfcl_bytes | irfcl_status | n_flows | status |
|---|---|---|---|---|---|---|
|  | 435470 |  |  |  |  | 200 |
|  |  |  |  |  | 222 |  |
|  |  |  | 3287 | 200 |  |  |
|  |  | 0 |  |  |  |  |
| 46 |  |  |  |  |  |  |

## Log
## A. IMF dataflow enumeration (what exists now?)

- `02:34:19`   monetary-ish flows: BOP, BOP_2026_APR_VINTAGE, BOP_2026_FEB_VINTAGE, BOP_2026_JAN_VINTAGE, BOP_2026_MAY_VINTAGE, BOP_AGG, CPI, CPI_2026_APR_VINTAGE, CPI_2026_FEB_VINTAGE, CPI_2026_JAN_VINTAGE, CPI_2026_MAY_VINTAGE, CPI_WCA, CPI_WCA_2026_APR_VINTAGE, CPI_WCA_2026_FEB_VINTAGE, CPI_WCA_2026_JAN_VINTAGE, CPI_WCA_2026_MAY_VINTAGE, FSIBSIS, FSIC, FSICDM, FSI_COUNTRY_METADATA_TABLE_2, GFS_BS, GFS_COFOG, GFS_SFCP, GFS_SOEF, GFS_SOO, GFS_SSUC, IRFCL, MFS_CBS, MFS_CBS_2026_APR_VINTAGE, MFS_CBS_2026_FEB_VINTAGE, MFS_CBS_2026_JAN_VINTAGE, MFS_CBS_2026_MAY_VINTAGE, MFS_DC, MFS_DC_2026_APR_VINTAGE, MFS_DC_2026_FEB_VINTAGE, MFS_DC_2026_JAN_VINTAGE, MFS_DC_2026_MAY_VINTAGE, MFS_FC, MFS_FC_2026_APR_VINTAGE, MFS_FC_2026_FEB_VINTAGE
- `02:34:19`   first 40 all: FSI_COUNTRY_METADATA_TABLE_2, FSICDM, MFS_CBS_2026_MAY_VINTAGE, GFS_SSUC, IL_2026_JAN_VINTAGE, ITG_2026_JAN_VINTAGE, IMTS_2026_MAY_VINTAGE, FSIBSIS, MFS_MA_2026_JAN_VINTAGE, QNEA, PI_2026_JAN_VINTAGE, QNEA_2026_MAY_VINTAGE, MFS_NSRF, RE, ER_2026_APR_VINTAGE, FDI, BOP_2026_MAY_VINTAGE, MFS_FC_2026_FEB_VINTAGE, EQ, WPFXI, SPE_2026_MAY_VINTAGE, ECFIE, EER_2026_MAY_VINTAGE, SDG, INFORMRISK, PI_WCA_2026_APR_VINTAGE, APDREO, RSUI, FA_2026_JAN_VINTAGE, MFS_ODC, MFS_CBS_2026_JAN_VINTAGE, PI, MFS_IR_2026_FEB_VINTAGE, MFS_MA_2026_MAY_VINTAGE, IL, CPI, ISORA_2018_DATA_PUB, QGFS_2026_APR_VINTAGE, MFS_IR_2026_APR_VINTAGE, ANEA_2026_MAY_VINTAGE
## B. IRFCL bulk — explicit status this time

- `02:34:20`   spots BR=None PE=None JP=None US=None
## C. BIS CBPOL — policy rates, all countries, daily

- `02:34:20`   https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/D..?lastNO... -> 406 (363B)
- `02:34:20`     body: <?xml version="1.0" ?>
<message:Error xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" xmlns:com="htt
- `02:34:21`   https://stats.bis.org/api/v1/data/WS_CBPOL/D../all?lastNObservations=1... -> 200 (32021B)
- `02:34:21`   spots BR=14.25 PE=4.25 NO=4.25 JP=1.0 US=3.625
## D. WB breadth across doctrine families

- `02:34:21`   FER    FI.RES.TOTL.CD         -> 183 countries, latest~2025
- `02:34:21`   GDG    GC.DOD.TOTL.GD.ZS      -> 120 countries, latest~2007
- `02:34:42`   BM     FM.LBL.BMNY.CN         ->   0 countries, latest~
- `02:35:00`   LEND   FR.INR.LEND            -> 148 countries, latest~2017
- `02:35:00`   GDPYY  NY.GDP.MKTP.KD.ZG      -> 261 countries, latest~2025
- `02:35:00`   IRYY   FP.CPI.TOTL.ZG         -> 240 countries, latest~2025
- `02:35:00`   UR     SL.UEM.TOTL.ZS         -> 234 countries, latest~2025
- `02:35:00` ✅ DISCOVERY2 DONE — flows enumerated, BIS + WB measured
