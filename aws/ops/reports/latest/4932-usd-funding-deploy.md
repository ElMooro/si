# ops 4932 — justhodl-usd-funding deploy (in-ops) + accept

**Status:** success  
**Duration:** 34.5s  
**Finished:** 2026-08-20T04:09:23+00:00  

## Data

| abcp_share_pct | body | bytes | claims_usd_bn | cp_cells | cp_cells_live | declared_gaps | duration_s | errors | feed_bytes | feed_ok | feed_status | fn_error | function_existed_before | function_exists_after | generated | ids | key_used | legs | liabilities_usd_bn | marker_present | mean_z | missing | net_usd_bn | page_status | period | quality_spread_30d_bp | reading | status | status_code | sum_z | term_slope_bp | verdict | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  | False |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | {'statusCode': 200, 'body': '{"status": "PARTIAL", "errors": 5, "sum_z": 0.22}'} |  |  |  |  |  |  |  |  |  |  | none |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 200 |  |  |  |  |
|  |  |  |  |  |  |  |  | 5 |  |  |  |  |  |  | 2026-08-20T04:08:56 |  |  |  |  |  |  |  |  |  |  |  |  | PARTIAL |  |  |  |  | 1.0.0 |
| 34.6 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 25.0 |  |  |  |  |  |  |  |
|  |  |  |  | 13 | 13 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2.0 |  |  |
|  |  |  | 3934.1 |  |  |  |  |  |  |  |  |  |  |  |  |  | Q.S.{pos}.A.USD....... |  | 49.2 |  |  |  | 3884.9 |  | 2026-Q1 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1 |  |  | 0.22 | SOFR-IORB,OBFR-IORB,TGCR-IORB,BGCR-IORB |  |  |  |  | NORMAL |  |  | 0.22 |  |  |  |
|  |  |  |  |  |  | 2 |  |  |  |  |  |  |  |  |  | sofr_futures,xccy_basis |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | 31999 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  | 200 |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 9260 | True | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 34 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | RED |  |

## Log
## 0. Env inheritance

- `04:08:49` ✅ inherited FRED_API_KEY from dollar-strength-agent (len 32)
## 0b. Pre-state

- `04:08:49`   zip: 107029 bytes
## 1. Lambda

- `04:08:49`   Lambda missing — creating
- `04:08:54` ✅   ✓ created justhodl-usd-funding
## 2. EB rule + permissions

- `04:08:55` ✅   ✓ created rule justhodl-usd-funding-daily
- `04:08:55` ✅   ✓ target → justhodl-usd-funding
- `04:08:55` ✅   ✓ added invoke permission
## 0c. Post-state — creation VERIFIED, not inferred

## 1. Invoke

## 2. Live payload

- `04:09:23` ⚠ collector errors: nyfed:sofr:HTTPError: HTTP Error 400: Bad Request · nyfed:tgcr:HTTPError: HTTP Error 400: Bad Request · nyfed:bgcr:HTTPError: HTTP Error 400: Bad Request · nyfed:effr:HTTPError: HTTP Error 400: Bad Request · nyfed:obfr:HTTPError: HTTP Error 400: Bad Request
## G1 — TGCR/BGCR direct (absent from FRED entirely)

- `04:09:23` ✗ sofr NOT collected: HTTPError: HTTP Error 400: Bad Request
- `04:09:23` ✗ tgcr NOT collected: HTTPError: HTTP Error 400: Bad Request
- `04:09:23` ✗ bgcr NOT collected: HTTPError: HTTP Error 400: Bad Request
- `04:09:23` ✗ effr NOT collected: HTTPError: HTTP Error 400: Bad Request
- `04:09:23` ✗ obfr NOT collected: HTTPError: HTTP Error 400: Bad Request
- `04:09:23` ✗ G0 field: tgcr.volume_bn not numeric
- `04:09:23` ✗ G0 field: bgcr.volume_bn not numeric
## G2 — H.8 bank wholesale funding

- `04:09:23`   Large time deposits                         2,558 $bn  13wΔ 26.59      z 0.25    2026-08-05 [LTDACBW027SBOG]
- `04:09:23`   Borrowings                              2,294,729 $mn  13wΔ -32522.8   z -0.38   2026-08-05 [H8B3094NCBA]
- `04:09:23`   Total deposits                             19,496 $bn  13wΔ 292.72     z 0.45    2026-08-05 [DPSACBW027SBOG]
- `04:09:23`   Other deposits                             16,938 $bn  13wΔ 266.12     z 0.35    2026-08-05 [ODSACBW027SBOG]
- `04:09:23`   Net due to related foreign offices            344 $bn  13wΔ -90.97     z -0.91   2026-08-05 [NDFACBW027SBOG]
## G3 — CP quantities + tenor grid

- `04:09:23`   Commercial paper outstanding, total $   1428.7bn  4wΔ 41.06     13wΔ -2.47     (-0.17%) 2026-08-12
- `04:09:23`   Financial CP outstanding           $    632.7bn  4wΔ 27.29     13wΔ -9.68     (-1.51%) 2026-08-12
- `04:09:23`   Nonfinancial CP outstanding        $    301.8bn  4wΔ 0.4       13wΔ -28.71    (-8.69%) 2026-08-12
- `04:09:23`   Asset-backed CP outstanding        $    494.3bn  4wΔ 13.37     13wΔ 35.92     (7.84%) 2026-08-12
- `04:09:23`   A2/P2 nonfinancial     30d    3.910%  −SOFR 26.0      2026-08-18
- `04:09:23`   A2/P2 nonfinancial     7d     3.820%  −SOFR 17.0      2026-08-18
- `04:09:23`   A2/P2 nonfinancial     90d    4.010%  −SOFR 36.0      2026-08-10
- `04:09:23`   A2/P2 nonfinancial     O/N    3.800%  −SOFR 15.0      2026-08-18
- `04:09:23`   AA financial           30d    3.680%  −SOFR 3.0       2026-08-12
- `04:09:23`   AA financial           7d     3.670%  −SOFR 2.0       2026-08-14
- `04:09:23`   AA financial           90d    3.790%  −SOFR 14.0      2026-08-18
- `04:09:23`   AA financial           O/N    3.620%  −SOFR -3.0      2026-08-18
- `04:09:23`   AA nonfinancial        30d    3.660%  −SOFR 1.0       2026-08-18
- `04:09:23`   AA nonfinancial        90d    3.690%  −SOFR 4.0       2026-08-18
- `04:09:23`   AA nonfinancial        O/N    3.640%  −SOFR -1.0      2026-08-18
- `04:09:23`   AA asset-backed        30d    3.790%  −SOFR 14.0      2026-08-18
- `04:09:23`   AA asset-backed        90d    3.870%  −SOFR 22.0      2026-08-18
## G4 — SOFR term structure

- `04:09:23`   30-day average SOFR              3.640850  2026-08-19
- `04:09:23`   90-day average SOFR              3.637060  2026-08-19
- `04:09:23`   180-day average SOFR             3.660560  2026-08-19
- `04:09:23`   SOFR Index (compounded)          1.255406  2026-08-19
## G5 — BIS LBS USD cross-border positions

- `04:09:23`   aggregation: L_REP_CTY=5A (all reporting) × L_CP_COUNTRY=5J (all counterparties) × L_CP_SECTOR=A (all sectors), L_DENOM=USD
## G6 — z-score composite

- `04:09:23`   z(90d AA financial CP)         z +0.22
- `04:09:23` ✗ composite has only 1 legs
## G7 — declared gaps carry no fabricated value

## G8 — edge acceptance (explicit UA)

## Verdict

- `04:09:23` ✗ FAILED GATES: rate:sofr, rate:tgcr, rate:bgcr, rate:effr, rate:obfr, g0:tgcr, g0:bgcr, z:legs
