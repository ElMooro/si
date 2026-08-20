# ops 4925 -- floor-audit v2.1.0 retail guards

**Status:** failure  
**Duration:** 87.0s  
**Finished:** 2026-08-20T02:40:03+00:00  

## Error

```
SystemExit: 1
```

## Data

| alerts | approx_cov | as_of | deep | frame | frames_merged | g0_ok | g1 | g2 | g3 | g4 | max_deep | mcap_musd | memory | min_mcap_usd | prescreen_min_cov | promoted | promoted_names | screen | screened | tier | tier_large | tier_mega | tier_micro | tier_mid | tier_nano | tier_small | timeout | truncated |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | PASS |  |  |  |  |  | 2048 |  |  |  |  |  |  |  |  |  |  |  |  |  | 900 |  |
|  |  |  |  |  |  |  |  | PASS |  |  | 120 |  |  | 15000000.0 | 0.4 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 82 |  | 2026-08-20T02:40:00+00:00 | 168 |  |  | 163 |  |  | PASS |  |  |  |  |  |  |  |  |  | 2858 |  |  |  |  |  |  |  |  | 0 |
|  |  |  |  | shares | CY2026Q3I(85),CY2026Q2I(4375) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | prices | 2026-08-19 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | cash | CY2026Q3I(1),CY2026Q2I(3868) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | cash_ifrs | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | st_inv | CY2026Q2I(433),CY2026Q1I(482),CY2025Q4I(661),CY2025Q3I(497) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | lt_inv | CY2026Q2I(250),CY2026Q1I(284),CY2025Q4I(441),CY2025Q3I(301) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | crypto | CY2026Q2I(122),CY2026Q1I(121),CY2025Q4I(117),CY2025Q3I(8) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | debt_nc | CY2026Q2I(1359),CY2026Q1I(1496),CY2025Q4I(1718) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | debt_c | CY2026Q2I(1053),CY2026Q1I(1133),CY2025Q4I(1410) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | st_borrow | CY2026Q3I(1),CY2026Q2I(470),CY2026Q1I(517),CY2025Q4I(732),CY2025Q3I(549) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | rpo | CY2026Q2I(597),CY2026Q1I(658),CY2025Q4I(747),CY2025Q3I(667) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  | 2858 |  | 528 | 45 | 534 | 748 | 191 | 812 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 120 | ACRV,ADCT,AFCG,AHT,AIFA,APUS,ARI,ATLCP,ATNM,ATOS,ATYR,AVX,BFH,BNC,BNO,BOLD,BRNS,BRR,BUR,BXDC |  |  |  |  |  |  |  |  |  |  |  |
|  | 7.6234 |  |  |  |  |  |  |  |  |  |  | 16.7 |  |  |  |  |  | WETH |  | nano |  |  |  |  |  |  |  |  |
|  | 5.8281 |  |  |  |  |  |  |  |  |  |  | 73.3 |  |  |  |  |  | AIFC |  | micro |  |  |  |  |  |  |  |  |
|  | 4.2751 |  |  |  |  |  |  |  |  |  |  | 21.7 |  |  |  |  |  | AVX |  | nano |  |  |  |  |  |  |  |  |
|  | 3.3663 |  |  |  |  |  |  |  |  |  |  | 166.5 |  |  |  |  |  | CNTN |  | micro |  |  |  |  |  |  |  |  |
|  | 3.3555 |  |  |  |  |  |  |  |  |  |  | 21.6 |  |  |  |  |  | AHT |  | nano |  |  |  |  |  |  |  |  |
|  | 3.2987 |  |  |  |  |  |  |  |  |  |  | 35.2 |  |  |  |  |  | QNCX |  | nano |  |  |  |  |  |  |  |  |
|  | 3.2928 |  |  |  |  |  |  |  |  |  |  | 29.0 |  |  |  |  |  | KYNB |  | nano |  |  |  |  |  |  |  |  |
|  | 2.9229 |  |  |  |  |  |  |  |  |  |  | 109.3 |  |  |  |  |  | BNC |  | micro |  |  |  |  |  |  |  |  |
|  | 2.8077 |  |  |  |  |  |  |  |  |  |  | 54.6 |  |  |  |  |  | SKYA |  | micro |  |  |  |  |  |  |  |  |
|  | 2.7509 |  |  |  |  |  |  |  |  |  |  | 108.4 |  |  |  |  |  | NAKA |  | micro |  |  |  |  |  |  |  |  |

## Log
## G1 deploy

- `02:38:36`   zip: 118370 bytes
## 1. Lambda

- `02:38:36`   Lambda exists — updating
- `02:38:42` ✅   ✓ updated justhodl-floor-audit
## G2 config reset

## G3 fresh run

## G4 market breadth

## G5 decision layer

- `02:40:03` FAIL G5: buy call on an untradable name: [('QNCX', 0), ('AIFA', 0), ('INHD', 0), ('INVE', 0), ('CUBI', 0), ('SCLX', 0), ('GPMT', 0)]
