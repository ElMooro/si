# ops 4935 — justhodl-usd-funding deploy (in-ops) + accept

**Status:** success  
**Duration:** 37.0s  
**Finished:** 2026-08-20T16:13:29+00:00  

## Data

| abcp_share_pct | bis_ambiguous | body | bytes | claims_liab_ratio | claims_usd_bn | cp_cells | cp_cells_live | declared_gaps | duration_s | errors | feed_bytes | feed_ok | feed_status | fn_error | function_existed_before | function_exists_after | generated | ids | key_pinned | key_used | legs | liabilities_usd_bn | marker_present | mean_z | missing | net_usd_bn | page_status | period | quality_spread_30d_bp | reading | status | status_code | sum_z | term_slope_bp | verdict | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | {'statusCode': 200, 'body': '{"status": "GREEN", "errors": 0, "sum_z": 2.57}'} |  |  |  |  |  |  |  |  |  |  |  | none |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 200 |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 0 |  |  |  |  |  |  | 2026-08-20T16:13:03 |  |  |  |  |  |  |  |  |  |  |  |  |  | GREEN |  |  |  |  | 1.1.0 |
| 34.6 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 25.0 |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 13 | 13 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.8 |  |  |
|  | False |  |  | 1.12 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | Q.S.{pos}.A.USD.A.5J.A.5A.A.5J.N |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | 21796.9 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | Q.S.{pos}.A.USD.A.5J.A.5A.A.5J.N |  | 19518.5 |  |  |  | 2278.4 |  | 2026-Q1 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 5 |  |  | 0.51 | none |  |  |  |  | TIGHTENING |  |  | 2.57 |  |  |  |
|  |  |  |  |  |  |  |  | 2 |  |  |  |  |  |  |  |  |  | sofr_futures,xccy_basis |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 26039 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  | 200 |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 10288 | True | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 36 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | GREEN |  |

## Log
## 0. Env inheritance

- `16:12:53` ✅ inherited FRED_API_KEY from dollar-strength-agent (len 32)
## 0b. Pre-state

- `16:12:53`   zip: 108619 bytes
## 1. Lambda

- `16:12:53`   Lambda exists — updating
- `16:12:59` ✅   ✓ updated justhodl-usd-funding
## 2. EB rule + permissions

- `16:12:59`   rule already correct: justhodl-usd-funding-daily (cron(20 22 ? * MON-FRI *))
- `16:12:59` ✅   ✓ target → justhodl-usd-funding
- `16:13:00` ✅   ✓ added invoke permission
## 0c. Post-state — creation VERIFIED, not inferred

## 0d. NY Fed request-shape ladder, FROM INSIDE LAMBDA

- `16:13:02`   sofr  shape_used=last/750   rows=750
- `16:13:02`        last/750     ok rows=750
- `16:13:02`   tgcr  shape_used=last/750   rows=750
- `16:13:02`        last/750     ok rows=750
- `16:13:02`   bgcr  shape_used=last/750   rows=750
- `16:13:02`        last/750     ok rows=750
- `16:13:02`   effr  shape_used=last/750   rows=750
- `16:13:02`        last/750     ok rows=750
- `16:13:02`   obfr  shape_used=last/750   rows=750
- `16:13:02`        last/750     ok rows=750
## 1. Invoke

## 2. Live payload

## G1 — TGCR/BGCR direct (absent from FRED entirely)

- `16:13:29`   SOFR   3.6200%  vol $2923bn   IQR 7.0    fan 12.0   −IORB -3.0bp   z 0.28    2026-08-19
- `16:13:29`   TGCR   3.6000%  vol $1202bn   IQR 0.0    fan 12.0   −IORB -5.0bp   z 0.28    2026-08-19
- `16:13:29`   BGCR   3.6000%  vol $1228bn   IQR 0.0    fan 11.0   −IORB -5.0bp   z 0.27    2026-08-19
- `16:13:29`   EFFR   3.6300%  vol $95bn     IQR 1.0    fan 9.0    −IORB -2.0bp   z 1.44    2026-08-19
- `16:13:29`   OBFR   3.6300%  vol $216bn    IQR 1.0    fan 14.0   −IORB -2.0bp   z 1.52    2026-08-19
## G2 — H.8 bank wholesale funding

- `16:13:29`   Large time deposits                         2,558 $bn  13wΔ 26.59      z 0.25    2026-08-05 [LTDACBW027SBOG]
- `16:13:29`   Borrowings                              2,294,729 $mn  13wΔ -32522.8   z -0.38   2026-08-05 [H8B3094NCBA]
- `16:13:29`   Total deposits                             19,496 $bn  13wΔ 292.72     z 0.45    2026-08-05 [DPSACBW027SBOG]
- `16:13:29`   Other deposits                             16,938 $bn  13wΔ 266.12     z 0.35    2026-08-05 [ODSACBW027SBOG]
- `16:13:29`   Net due to related foreign offices            344 $bn  13wΔ -90.97     z -0.91   2026-08-05 [NDFACBW027SBOG]
## G3 — CP quantities + tenor grid

- `16:13:29`   Commercial paper outstanding, total $   1428.7bn  4wΔ 41.06     13wΔ -2.47     (-0.17%) 2026-08-12
- `16:13:29`   Financial CP outstanding           $    632.7bn  4wΔ 27.29     13wΔ -9.68     (-1.51%) 2026-08-12
- `16:13:29`   Nonfinancial CP outstanding        $    301.8bn  4wΔ 0.4       13wΔ -28.71    (-8.69%) 2026-08-12
- `16:13:29`   Asset-backed CP outstanding        $    494.3bn  4wΔ 13.37     13wΔ 35.92     (7.84%) 2026-08-12
- `16:13:29`   A2/P2 nonfinancial     30d    3.910%  −SOFR 29.0      2026-08-18
- `16:13:29`   A2/P2 nonfinancial     7d     3.820%  −SOFR 20.0      2026-08-18
- `16:13:29`   A2/P2 nonfinancial     90d    4.010%  −SOFR 39.0      2026-08-10
- `16:13:29`   A2/P2 nonfinancial     O/N    3.800%  −SOFR 18.0      2026-08-18
- `16:13:29`   AA financial           30d    3.680%  −SOFR 6.0       2026-08-12
- `16:13:29`   AA financial           7d     3.670%  −SOFR 5.0       2026-08-14
- `16:13:29`   AA financial           90d    3.790%  −SOFR 17.0      2026-08-18
- `16:13:29`   AA financial           O/N    3.620%  −SOFR 0.0       2026-08-18
- `16:13:29`   AA nonfinancial        30d    3.660%  −SOFR 4.0       2026-08-18
- `16:13:29`   AA nonfinancial        90d    3.690%  −SOFR 7.0       2026-08-18
- `16:13:29`   AA nonfinancial        O/N    3.640%  −SOFR 2.0       2026-08-18
- `16:13:29`   AA asset-backed        30d    3.790%  −SOFR 17.0      2026-08-18
- `16:13:29`   AA asset-backed        90d    3.870%  −SOFR 25.0      2026-08-18
## G4 — SOFR term structure

- `16:13:29`   30-day average SOFR              3.642520  2026-08-20
- `16:13:29`   90-day average SOFR              3.638290  2026-08-20
- `16:13:29`   180-day average SOFR             3.660340  2026-08-20
- `16:13:29`   SOFR Index (compounded)          1.255532  2026-08-20
## G5 — BIS LBS USD cross-border positions

- `16:13:29`   aggregation: L_CURR_TYPE=A (all) × L_PARENT_CTY=5J × L_REP_BANK_TYPE=A × L_REP_CTY=5A × L_CP_SECTOR=A × L_CP_COUNTRY=5J × L_POS_TYPE=N (cross-border) × L_DENOM=USD
## G6 — z-score composite

- `16:13:29`   z(SOFR − IORB)                 z +0.28
- `16:13:29`   z(OBFR − IORB)                 z +1.52
- `16:13:29`   z(TGCR − IORB)                 z +0.28
- `16:13:29`   z(BGCR − IORB)                 z +0.27
- `16:13:29`   z(90d AA financial CP)         z +0.22
## G7 — declared gaps carry no fabricated value

## G8 — edge acceptance (explicit UA)

## Verdict

- `16:13:29` ✅ all gates green
