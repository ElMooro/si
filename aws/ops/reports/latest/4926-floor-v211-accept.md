# ops 4926 -- floor-audit v2.1.1 acceptance

**Status:** success  
**Duration:** 122.7s  
**Finished:** 2026-08-20T02:46:43+00:00  

## Data

| act_ACCUMULATE | act_AVOID | act_BUY | act_NO_CALL | act_PASS | act_REDUCE | act_WATCH | action | alerts | approx_cov | as_of | attempts | btbt | btbt_call | btbt_durability | btbt_runway | call | contract_floors | conviction | cov | deep | definitions | durability | duration_s | feed_version | frame | frames_merged | g0_ok | g1 | g2 | g3 | g4 | g5 | g6 | g7 | guards | liquidity_unknown | marker | max_deep | mcap_musd | memory | min_mcap_usd | names | prescreen_min_cov | promoted | promoted_names | quality | quarantined | runway | screen | screened | status | thin_names | tier | tier_large | tier_mega | tier_micro | tier_mid | tier_nano | tier_small | timeout | truncated | unbound_debt_names | why |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  | 2048 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 900 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  | 120 |  |  | 15000000.0 |  | 0.4 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 82 |  | 2026-08-20T02:46:10+00:00 |  |  |  |  |  |  |  |  |  | 168 |  |  |  |  |  |  | 163 |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2858 |  |  |  |  |  |  |  |  |  |  | 0 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | shares | CY2026Q3I(85),CY2026Q2I(4375) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | prices | 2026-08-19 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | cash | CY2026Q3I(1),CY2026Q2I(3868) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | cash_ifrs | None |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | st_inv | CY2026Q2I(433),CY2026Q1I(482),CY2025Q4I(661),CY2025Q3I(497) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | lt_inv | CY2026Q2I(250),CY2026Q1I(284),CY2025Q4I(441),CY2025Q3I(301) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | crypto | CY2026Q2I(122),CY2026Q1I(121),CY2025Q4I(117),CY2025Q3I(8) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | debt_nc | CY2026Q2I(1359),CY2026Q1I(1496),CY2025Q4I(1718) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | debt_c | CY2026Q2I(1053),CY2026Q1I(1133),CY2025Q4I(1410) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | st_borrow | CY2026Q3I(1),CY2026Q2I(470),CY2026Q1I(517),CY2025Q4I(732),CY2025Q3I(549) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | rpo | CY2026Q2I(597),CY2026Q1I(658),CY2025Q4I(747),CY2025Q3I(667) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2858 |  |  |  | 528 | 45 | 534 | 748 | 191 | 812 |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 120 | ACRV,ADCT,AFCG,AHT,AIFA,APUS,ARI,ATLCP,ATNM,ATOS,ATYR,AVX,BFH,BNC,BNO,BOLD,BRNS,BRR,BUR,BXDC |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 7.6234 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 16.7 |  |  |  |  |  |  |  |  |  | WETH |  |  |  | nano |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 5.8281 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 73.3 |  |  |  |  |  |  |  |  |  | AIFC |  |  |  | micro |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 4.2751 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 21.7 |  |  |  |  |  |  |  |  |  | AVX |  |  |  | nano |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 3.3663 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 166.5 |  |  |  |  |  |  |  |  |  | CNTN |  |  |  | micro |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 3.3555 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 21.6 |  |  |  |  |  |  |  |  |  | AHT |  |  |  | nano |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 3.2987 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 35.2 |  |  |  |  |  |  |  |  |  | QNCX |  |  |  | nano |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 3.2928 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 29.0 |  |  |  |  |  |  |  |  |  | KYNB |  |  |  | nano |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 2.9229 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 109.3 |  |  |  |  |  |  |  |  |  | BNC |  |  |  | micro |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 2.8077 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 54.6 |  |  |  |  |  |  |  |  |  | SKYA |  |  |  | micro |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 2.7509 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 108.4 |  |  |  |  |  |  |  |  |  | NAKA |  |  |  | micro |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 32 |  |  |  |  |  |  |  |  |  | 48 |  |
| 19 | 50 | 7 | 18 | 16 | 0 | 53 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | BUY |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | AIFA,CUBI,GPMT,INHD,INVE,QNCX,SCLX |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | ACCUMULATE |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ADCT,ATYR,BTBT,CNC,DXC,FGNX,FLG,IZEA,LAB,MED,MVIS,PEW |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | AVOID |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ABTC,ACRV,AHT,AIFC,AVX,BMNR,BNC,BNO,BTCS,CALC,CDLX,CLRB |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | BUY |  |  |  |  |  |  |  |  | INHD |  | 92 | 1.8021 |  |  | 85 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 99 |  | 40.6 |  |  |  |  | nano |  |  |  |  |  |  |  |  |  | market cap is BELOW the net liquid assets: 180 cents of floor per dollar of pric |
|  |  |  |  |  |  |  | BUY |  |  |  |  |  |  |  |  | INVE |  | 91 | 1.7232 |  |  | 85 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 99 |  | 134.0 |  |  |  |  | micro |  |  |  |  |  |  |  |  |  | market cap is BELOW the net liquid assets: 172 cents of floor per dollar of pric |
|  |  |  |  |  |  |  | WATCH |  |  |  |  |  |  |  |  | OPAD |  | 91 | 1.9345 |  |  | 85 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 93 |  | 44.6 |  |  |  |  | nano |  |  |  |  |  |  |  |  |  | market cap is BELOW the net liquid assets: 193 cents of floor per dollar of pric |
|  |  |  |  |  |  |  | WATCH |  |  |  |  |  |  |  |  | NEON |  | 89 | 1.4195 |  |  | 85 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 98 |  | 38.0 |  |  |  |  | nano |  |  |  |  |  |  |  |  |  | market cap is BELOW the net liquid assets: 142 cents of floor per dollar of pric |
|  |  |  |  |  |  |  | WATCH |  |  |  |  |  |  |  |  | TTEC |  | 89 | 5.8237 |  |  | 90 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 61 |  | self_funding |  |  |  |  | micro |  |  |  |  |  |  |  |  |  | market cap is BELOW the net liquid assets: 582 cents of floor per dollar of pric |
|  |  |  |  |  |  |  | BUY |  |  |  |  |  |  |  |  | SCLX |  | 85 | 1.5175 |  |  | 82 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 73 |  | self_funding |  |  |  |  | nano |  |  |  |  |  |  |  |  |  | market cap is BELOW the net liquid assets: 152 cents of floor per dollar of pric |
|  |  |  |  |  |  |  | WATCH |  |  |  |  |  |  |  |  | LFT |  | 84 | 0.9081 |  |  | 90 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 100 |  | self_funding |  |  |  |  | nano |  |  |  |  |  |  |  |  |  | 91% of the market cap is already covered by net liquid assets |
|  |  |  |  |  |  |  | AVOID |  |  |  |  |  |  |  |  | MARA |  | 80 | 0.1854 |  |  | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 80 |  | 10.0 |  |  |  |  | mid |  |  |  |  |  |  |  |  |  | 41 points of the drawdown are not explained by any move in the assets themselves |
|  |  |  |  |  |  |  |  |  |  |  |  | SENSELESS_DRAWDOWN | ACCUMULATE | 82 | None |  | ACM,BA,FSLR,UIS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  | 18 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 2 |  |  |  |  |  |  |  |  |  | served |  |  | 2.1.1 |  |  |  |  |  |  |  |  |  | PASS |  |  | floor-audit-v2.1.1 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 122 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | GREEN |  |  |  |  |  |  |  |  |  |  |  |  |

## Log
## G1 deploy

- `02:44:41`   zip: 118376 bytes
## 1. Lambda

- `02:44:41`   Lambda exists — updating
- `02:44:45` ✅   ✓ updated justhodl-floor-audit
## G2 config reset

## G3 fresh run

## G4 market breadth

## G5 decision layer

## G6 precedence + regression

## G7 edge

