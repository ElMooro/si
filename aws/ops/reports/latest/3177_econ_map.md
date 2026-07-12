# ops 3177 — ECONOMICS mapped → dormant engines wake

**Status:** success  
**Duration:** 600.1s  
**Finished:** 2026-07-12T23:40:52+00:00  

## Error

```
SystemExit: 0
```

## Data

| active_now | coverage_before_pct | coverage_now_pct | dormant | elapsed_s | engines | fdr | firing | n_fails | n_warns | series_cached | signals | src_coingecko | src_formula | src_fred | src_market | src_unmapped | src_worldbank | unique_symbols | verdict | wb_live | wb_probes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 6 | 6 |
|  | 44.5 | 59.7 |  |  |  |  |  |  |  |  |  | 37 | 337 | 782 | 1739 | 2624 | 988 | 6507 |  |  |  |
| 96 |  |  | 71 | 268.0 | 167 | 0 | 20 |  |  | 1705 | 0 |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0 | 0 |  |  |  |  |  |  |  |  |  | PASS |  |  |

## Log
## 1. World Bank probes (does the data actually come back?)

- `23:30:53`   Brazil debt/GDP            2010 → 2024  (15 obs)
- `23:30:54`   Zimbabwe deposit rate      1990 → 2025  (30 obs)
- `23:30:54`   Cambodia trade balance     1993 → 2025  (33 obs)
- `23:30:55`   China FX reserves          1990 → 2025  (36 obs)
- `23:30:55`   Japan GDP growth           1990 → 2025  (36 obs)
- `23:30:56`   India CPI                  1990 → 2025  (36 obs)
## 2. Re-map the universe

- `23:36:05`   UNMAPPED      2624  (40.3%)
- `23:36:05`   MARKET        1739  (26.7%)
- `23:36:05`   WORLDBANK      988  (15.2%)
- `23:36:05`   FRED           782  (12.0%)
- `23:36:05`   FORMULA        337  (5.2%)
- `23:36:05`   COINGECKO       37  (0.6%)
- `23:36:05` ✅ symbol-map rewritten: 44.5% → 59.7% (+988 symbols)
## 3. Re-run the engine fleet on the wider map

- `23:36:05`   zip: 65701 bytes
## 1. Lambda

- `23:36:05`   Lambda exists — updating
- `23:36:08` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `23:36:09`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `23:36:09` ✅   ✓ target → justhodl-wl-engines
- `23:36:09` ✅   ✓ added invoke permission
- `23:40:52` ── themes: OTHER=62, LIQUIDITY=23, CREDIT=23, BREADTH=15, GROWTH=12, STRESS=11, RATES=10, DOLLAR=5, INFLATION=4, CRYPTO=2
- `23:40:52` ✅ ACTIVE engines 53 → 96 (+43 woken by the ECONOMICS map)
- `23:40:52` ── FIRING (20):
- `23:40:52`   Foreign Exchange Reserves        [LIQUIDITY] act  69.2% (100.0p) t= -0.82 lit: ECONOMICS:PTFER, ECONOMICS:DEFER, ECONOM
- `23:40:52`   82604570                         [OTHER    ] act  75.0% ( 96.1p) t=  0.44 lit: AMEX:IXN, FRED:W006RC1Q027SBEA, NASDAQ:D
- `23:40:52`   Frontier Market ETFS             [OTHER    ] act  85.7% ( 93.5p) t=  0.75 lit: NASDAQ:MSSCX, NASDAQ:MSSVX, NASDAQ:MSSYX
- `23:40:52`   Different Types of Stock indexes [BREADTH  ] act  92.9% ( 93.4p) t=  0.26 lit: NASDAQ:PTH, NASDAQ:PIE, NASDAQ:FYT, AMEX
- `23:40:52`   Commodities : are often rented b [INFLATION] act  78.9% ( 91.3p) t= -0.65 lit: FRED:PRAWMINDEXM, FRED:PALUMUSDM, FRED:P
- `23:40:52`   82577015                         [OTHER    ] act  50.0% ( 89.9p) t=  0.21 lit: NASDAQ:COOP, FRED:WFRBLB50107, NASDAQ:MA
- `23:40:52`   EuroDollar Predict future moves: [LIQUIDITY] act  47.4% ( 89.8p) t= -0.36 lit: NASDAQ:PIE, TVC:NI225, FX_IDC:USDINR, FX
- `23:40:52`   Energy and oil stocks            [BREADTH  ] act  77.3% ( 89.6p) t=  0.42 lit: NYSE:MPC, NYSE:INSW, NYSE:VLO, NYSE:PSX,
- `23:40:52`   EuroDollar banks                 [DOLLAR   ] act  77.8% ( 88.5p) t= -0.33 lit: NYSE:UBS, NYSE:MS, NYSE:HSBC, NYSE:C, NY
- `23:40:52`   Emerging Markets Liquidity: $ Fl [LIQUIDITY] act  81.8% ( 87.6p) t=  1.54 lit: AMEX:EEMO, AMEX:ECON, AMEX:EEMV, AMEX:XS
- `23:40:52`   fed powell holding               [RATES    ] act  70.0% ( 87.2p) t=   1.3 lit: AMEX:GSSC, AMEX:EEM, AMEX:SCHD, NYSE:IGD
- `23:40:52`   Global Commodities prices        [INFLATION] act  71.4% ( 86.9p) t= -2.28 lit: FRED:PRAWMINDEXM, FRED:PALUMUSDM, FRED:P
- `23:40:52`   Ai stocks                        [BREADTH  ] act  76.9% ( 85.5p) t= -0.29 lit: NASDAQ:AMAT, NASDAQ:ALAB, NASDAQ:MRVL, N
- `23:40:52`   Countries Balance of Trade       [OTHER    ] act  51.7% ( 84.8p) t=  1.45 lit: ECONOMICS:CRBOT, ECONOMICS:GNBOT, ECONOM
- `23:40:52`   Global ETFs                      [OTHER    ] act  71.0% ( 83.9p) t=  0.41 lit: AMEX:DEEP, NASDAQ:PIE, AMEX:PEXL, NASDAQ
