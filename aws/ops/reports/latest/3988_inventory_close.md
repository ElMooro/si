# ops 3988 — fleet inventory + v1.8 close

**Status:** failure  
**Duration:** 175.9s  
**Finished:** 2026-07-27T22:00:40+00:00  

## Error

```
SystemExit: 1
```

## Data

| generated_at | marker | n_engines | n_pages | per_source | scalar_paths | total_data_points_by_source | us10 | with_description | with_purpose |
|---|---|---|---|---|---|---|---|---|---|
| 2026-07-27T21:50:31.065006+00:00 | data-census v1.8 ops3987 clean-values |  |  | {"FLEET-INTERNAL": 7084, "OTHER": 3139, "FMP": 1251, "POLYGON": 696, "FRED": 488, "CFTC": 95, "ECB": 37, "COINMETRICS": 28, "YAHOO": 20, "US-TREASURY": 7, "MOEA-TAIWAN": 4, "SEC-EDGAR": 3, "BOE": 2, "GNEWS": 1, "PBOC": 1} | 250110 | 12856 |  |  |  |
|  |  |  |  |  |  |  | {"name": "US10Y", "value": 4.71, "live": true, "pulled_from": "fred_alias:DGS10", "engine": "justhodl-domain-barometers", "artifact": "data/domain-barometers.json", "path": "symbols[US10Y].value"} |  |  |
|  |  | 733 |  |  |  |  |  | 711 |  |
|  |  |  | 414 |  |  |  |  |  | 152 |

## Log
## A. the v1.8 artifact (fired 21:50:30, never gated)

- `21:57:45`   FRED: 10Y Breakeven Inflation Vol · realized_vol_now = 0.3146 live=True from FRED (Federal Reserve Bank of  via justhodl-bond-vol
- `21:57:45`   FRED: 10Y Real Yield Vol · realized_vol_now = 0.576 live=True from FRED (Federal Reserve Bank of  via justhodl-bond-vol
- `21:57:45`   FRED: 10Y Treasury Yield Vol · realized_vol_now = 0.677 live=True from FRED (Federal Reserve Bank of  via justhodl-bond-vol
- `21:57:45`   FRED: 10Y-3M Curve Vol · realized_vol_now = 0.5997 live=True from FRED (Federal Reserve Bank of  via justhodl-bond-vol
- `21:57:45`   FRED: 16% Trimmed-Mean CPI (1m ann.) = 2.63 live=True from FRED — Atlanta/Cleveland/Dalla via justhodl-nowcast-desk
- `21:57:45`   FRED: 2Y Treasury Yield Vol · realized_vol_now = 0.831 live=True from FRED (Federal Reserve Bank of  via justhodl-bond-vol
## B. WIRE engines (already generated fleet-wide)

## C. COMPILE data/page-manifest.json from the repo HTML

- `21:57:45`   13f.html                     | 13F · Big Investors · JustHodl         | What 18 of the world
- `21:57:45`   about.html                   | About · JustHodl AI                    | JustHodl.AI is an institutional-grade financial intelligence
- `21:57:45`   accumulation.html            | Accumulation / Distribution Radar · Ju | 
- `21:57:45`   accuracy.html                | Accuracy · JustHodl                    | Live calibration dashboard — every signal type
- `21:57:45`   activist-13d.html            | Activist 13D Scanner | JustHodl        | 
- `21:57:45`   activity-nowcast.html        | Activity Nowcast · JustHodl.AI         | 
## D. page v4 at the edge

- `21:57:45`   [0] 14181B 0/5
- `21:58:10`   [1] 14181B 0/5
- `21:58:35`   [2] 14181B 0/5
- `21:59:00`   [3] 14181B 0/5
- `21:59:25`   [4] 14181B 0/5
- `21:59:50`   [5] 14181B 0/5
- `22:00:15`   [6] 14181B 0/5
- `22:00:40`   [7] 17936B 5/5
- `22:00:40` ✅   artifact is v1.8
- `22:00:40` ✅   >=6 source families
- `22:00:40` ✅   FRED >=150 metrics
- `22:00:40` ✅   US10Y is a yield 0<v<25
- `22:00:40` ✗   US10Y from DGS10 via the vault engine
- `22:00:40` ✅   no tried_at junk anywhere in the directory
- `22:00:40` ✅   engine manifest >=600 engines
- `22:00:40` ✅   >=70% engines carry a description
- `22:00:40` ✅   page manifest >=350 pages
- `22:00:40` ✗   >=60% pages carry a purpose
- `22:00:40` ✅   page v4 live at edge
- `22:00:40` ✗ FAILED: ['US10Y from DGS10 via the vault engine', '>=60% pages carry a purpose']
