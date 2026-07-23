# ops 3752 — AAR rail carloads BY COMMODITY (canary #9) probe

**Status:** success  
**Duration:** 23.5s  
**Finished:** 2026-07-23T00:33:36+00:00  

## Data

| aar_ok | buildable | fred_series | stb_ok |
|---|---|---|---|
| 1 | True | 14 | 3 |

## Log
## A — FRED series search for rail/carload commodity splits

- `00:33:13` ✅   'rail carloads' -> 12 series
- `00:33:13`     RAILFRTCARLOADSD11           Rail Freight Carloads
- `00:33:13`     RAILFRTCARLOADS              Rail Freight Carloads
- `00:33:13`     PCU482111482111411           Producer Price Index by Industry: Line-Haul Railroads: Carload Freight Rail Tr
- `00:33:13`     PCU48211148211141111         Producer Price Index by Industry: Line-Haul Railroads: Carload Rail Transporta
- `00:33:13`     PCU4821114821111             Producer Price Index by Industry: Line-Haul Railroads: Rail Transportation, Fr
- `00:33:13`     PCU48211148211141105         Producer Price Index by Industry: Line-Haul Railroads: Carload Rail Transporta
- `00:33:13`     PCU48211148211141103         Producer Price Index by Industry: Line-Haul Railroads: Carload Rail Transporta
- `00:33:13`     PCU48211148211141110         Producer Price Index by Industry: Line-Haul Railroads: Carload Rail Transporta
- `00:33:14` ✅   'rail freight carloads chemicals' -> 0 series
- `00:33:14` ✅   'carloads motor vehicles' -> 2 series
- `00:33:14`     M03031USM343NNBR             Index of Freight Car loadings, Miscellaneous for United States
- `00:33:14`     M03002USM544NNBR             Freight Cars Loaded for United States
- `00:33:14` ✅   'rail intermodal' -> 6 series
- `00:33:14`     RAILFRTINTERMODAL            Rail Freight Intermodal Traffic
- `00:33:14`     RAILFRTINTERMODALD11         Rail Freight Intermodal Traffic
- `00:33:14`     PCU482111482111412           Producer Price Index by Industry: Line-Haul Railroads: Intermodal Freight Rail
- `00:33:14`     RAILFRTCARLOADSD11           Rail Freight Carloads
- `00:33:14`     PCU4821114821112             Producer Price Index by Industry: Line-Haul Railroads: Rail Transportation, Fr
- `00:33:14`     RAILFRTCARLOADS              Rail Freight Carloads
## B — AAR public weekly rail traffic

- `00:33:17` ⚠   https://www.aar.org/wp-content/uploads/2026/07/railtraffic.csv -> HTTP Error 404: Not Found
- `00:33:18` ✅   https://www.aar.org/data-center/rail-traffic-data/ -> HTTP 200 len=296019
- `00:33:18`     head: <!doctype html> <html lang="en-US">  <head> 	<meta charset="UTF-8"> 	<meta name="viewport" content="width=device-width, initial-scale=1"> 	<link rel="profile" href="http://gmpg.org/xfn/11">  	<!-- Goo
- `00:33:20` ⚠   https://www.aar.org/news/rail-traffic-data/ -> HTTP Error 404: Not Found
## C — Surface Transportation Board (federal, durable)

- `00:33:21` ✅   https://www.stb.gov/reports-data/economic-data/ -> HTTP 200 len=134011
- `00:33:21`     data link: https://www.stb.gov/wp-content/uploads/STB_49_CFR_1247_CARS_LOAD_TERM_RRRR_YEAR_YYYYMMDDHHMM.csv
- `00:33:21`     data link: https://www.stb.gov/wp-content/uploads/STB_REI_RRRR_YEAR_QQ_YYYYMMDDHHMM.csv
- `00:33:21`     data link: https://www.stb.gov/wp-content/uploads/STB_CBS_RRRR_YEAR_QQ_YYYYMMDDHHMM.csv
- `00:33:21`     data link: https://www.stb.gov/wp-content/uploads/STB_49_CFR_1245_RRRR_YEAR_QQ_YYYYMMDDHHMM.csv
- `00:33:21`     data link: https://www.stb.gov/wp-content/uploads/STB_49_CFR_1246_RRRR_YEAR_MO_YYYYMMDDHHMM.csv
- `00:33:21`     data link: /wp-content/uploads/RSAM-COMP-2024.xlsx
- `00:33:28` ✅   https://www.stb.gov/reports-data/rail-service-data/ -> HTTP 200 len=1796757
- `00:33:28`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-BNSF.csv
- `00:33:28`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-CPKC.csv
- `00:33:28`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-CSXT.csv
- `00:33:28`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-GTC.csv
- `00:33:28`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-NS.csv
- `00:33:28`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-UP.csv
- `00:33:36` ✅   https://prod.stb.gov/reports-data/rail-service-data/ -> HTTP 200 len=1796759
- `00:33:36`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-BNSF.csv
- `00:33:36`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-CPKC.csv
- `00:33:36`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-CSXT.csv
- `00:33:36`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-GTC.csv
- `00:33:36`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-NS.csv
- `00:33:36`     data link: https://www.stb.gov/wp-content/uploads/STB-1145-UP.csv
## D — the aggregate series we already carry (contrast)

- `00:33:36` ✅   RAILFRTCARLOADSD11 latest=1003578 (2026-04-01)
- `00:33:36` ✅   RAILFRTINTERMODALD11 latest=1222431 (2026-04-01)
## VERDICT

- `00:33:36`   distinct FRED series seen: 14
- `00:33:36`   AAR reachable: 1 · STB reachable: 3
- `00:33:36` ✅ PROBE COMPLETE — decide source from the evidence above
