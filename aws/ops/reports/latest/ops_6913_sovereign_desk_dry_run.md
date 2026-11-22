# ops 6913 - DRY RUN: the sovereign-stress desk document, compiled from live originals (nothing written), 2026-11-22T16:22+00:00

**Status:** success  
**Duration:** 72.4s  
**Finished:** 2026-11-22T16:23:25+00:00  

## Data

| checked_against_warehouse | countries | differ | document_gzip | failed_checks | history_gzip | measures | seconds |
|---|---|---|---|---|---|---|---|
| 58 | 30 | 0 | 161059 | 0 | 396803 | 266 | 71 |

## Log
## 1. Compile, as the Lambda will

- `16:22:16`   ECB YC/B.U2.EUR.4F.G_N_A+G_N_C.SV_C_YM.SR_2Y+SR_10Y?format=csvdata -> 12767481 bytes in 2.7s
- `16:22:22`   ECB IRS/M..L.L40.CI.0000..N.Z?format=csvdata -> 5689197 bytes in 4.2s
- `16:22:28`   ECB TGB/M...A094T.U2.EUR.E?format=csvdata -> 891532 bytes in 5.0s
- `16:22:33`   ECB GFS/Q.N..W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T?format=csv -> 2928190 bytes in 3.8s
- `16:22:39`   ECB GFS/Q.N..W0.S13.S1._Z.B.B9._Z._Z._Z.XDC_R_B1GQ_CY._Z.S.V.CY._T?format= -> 2251378 bytes in 4.7s
- `16:22:48`   ECB GFS/Q.N..W0.S13.S1._Z.B.B9P._Z._Z._Z.XDC_R_B1GQ_CY._Z.S.V.CY._T?format -> 2441315 bytes in 8.0s
- `16:22:57`   ECB GFS/Q.N..W0.S13.S1.C.D.D41._Z._Z._T.XDC_R_B1GQ_CY._Z.S.V.CY._T?format= -> 2212289 bytes in 7.8s
- `16:23:00`   ECB EXR/D.USD.EUR.SP00.A?format=csvdata -> 1492522 bytes in 1.8s
- `16:23:03`   ECB ILM/W.U2.C.A030000.U2.Z06?format=csvdata -> 540155 bytes in 1.5s
- `16:23:05`   ECB ILM/W.U2.C.A020000.U4.Z06?format=csvdata -> 528477 bytes in 1.4s
- `16:23:24` published (to the overlay) in 71s: {"published": true, "generated_at": "2026-11-22T16:23:06.102512+00:00", "coverage": {"measures": 266, "countries": 30, "errors": 0, "status": "measured"}, "replay": {"manifest_key": "data/ciss-research/sovereign/runs/19adfc11acf52cc561aee843174da2b7ff9d88318137098c9b1021bd7b8584a0.json", "output_sha
- `16:23:24` document 820800 bytes (161059 gzip) | history 2333035 bytes (396803 gzip) | 76 objects read from the bucket | writes held back: 7
- `16:23:24` what a live run would write: {"evidence": 3, "compiler": 1, "run receipt": 1, "data/ciss-sovereign-history.json": 1, "data/ciss-sovereign.json": 1}
- `16:23:24` errors: {} | coverage {"countries": 30, "errors": 0, "measures": 266, "status": "measured"} | ended at source: 4
- `16:23:24`     ended: CLIFS_EE   CLIFS.M.EE._Z.4F.EC.CLIFS_CI.IDX last 2010-12-31
- `16:23:24`     ended: DEBT_GB    GFS.Q.N.GB.W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T last 2024-06-30
- `16:23:24`     ended: DEBT_US    GFS.Q.N.US.W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T last 2024-06-30
- `16:23:24`     ended: Y10_GB     IRS.M.GB.L.L40.CI.0000.GBP.N.Z last 2020-01-31
## 2. The sources

- `16:23:24`   ciss             CISS, SovCISS and CLIFS (the warehouse) | 67 series, - rows, newest -
- `16:23:24`   curve            Euro-area government yield curve, AAA issuers and all issuers (daily) | 4 series, 22712 rows, newest 2026-11-19
- `16:23:24`   yields           10-year government yields by country, convergence statistic (monthly) | 34 series, 12175 rows, newest 2026-10
- `16:23:24`   target           TARGET balances of the national central banks, end of month | 23 series, 4871 rows, newest 2026-09
- `16:23:24`   debt             General government debt, percent of GDP (quarterly) | 34 series, 3998 rows, newest 2026-Q2
- `16:23:24`   deficit          Government deficit (-) or surplus (+), four-quarter sum, percent of GDP | 31 series, 3356 rows, newest 2026-Q2
- `16:23:24`   primary          Government primary balance (before interest), four-quarter sum, percent of GDP | 31 series, 3356 rows, newest 2026-Q2
- `16:23:24`   interest         Government interest expenditure, four-quarter sum, percent of GDP | 31 series, 3356 rows, newest 2026-Q2
- `16:23:24`   eurusd           Euro reference rate against the US dollar (daily) | 1 series, 7203 rows, newest 2026-11-20
- `16:23:24`   fx_claims_ea     Eurosystem claims on euro-area residents in foreign currency (weekly) | 1 series, 1455 rows, newest 2026-W46
- `16:23:24`   fx_claims_nonea  Eurosystem claims on non-euro-area residents in foreign currency (weekly) | 1 series, 1455 rows, newest 2026-W46
## 3. The measures, by group

- `16:23:24`   sovciss        11 measures ({"current": 11}) | newest 2026-11-19, oldest 2026-11-19
- `16:23:24`   sovciss_ended   6 measures ({"ended": 6}) | newest 2025-04-30, oldest 2025-04-30
- `16:23:24`   euro_area       7 measures ({"current": 7}) | newest 2026-11-19, oldest 2026-09-30
- `16:23:24`   contribution    6 measures ({"current": 6}) | newest 2026-11-19, oldest 2026-11-19
- `16:23:24`   ciss           13 measures ({"current": 13}) | newest 2026-11-19, oldest 2026-11-13
- `16:23:24`   clifs          27 measures ({"current": 27}) | newest 2026-09-30, oldest 2026-09-30
- `16:23:24`   curve           4 measures ({"current": 4}) | newest 2026-11-19, oldest 2026-11-19
- `16:23:24`   yield          28 measures ({"current": 28}) | newest 2026-10-31, oldest 2026-10-31
- `16:23:24`   spread         27 measures ({"current": 27}) | newest 2026-10-31, oldest 2026-10-31
- `16:23:24`   target         22 measures ({"current": 22}) | newest 2026-09-30, oldest 2026-09-30
- `16:23:24`   debt           28 measures ({"current": 28}) | newest 2026-06-30, oldest 2026-06-30
- `16:23:24`   deficit        28 measures ({"current": 28}) | newest 2026-06-30, oldest 2026-06-30
- `16:23:24`   primary        28 measures ({"current": 28}) | newest 2026-06-30, oldest 2026-06-30
- `16:23:24`   interest       28 measures ({"current": 28}) | newest 2026-06-30, oldest 2026-06-30
- `16:23:24`   auxiliary       3 measures ({"current": 3}) | newest 2026-11-20, oldest 2026-11-13
## 4. Against the warehouse's own rows

- `16:23:24`   58 measures checked against their warehouse rows, 0 differ
- `16:23:24`   blank newest row at the ECB: CISS_DE    shows 0.00798 on 2026-11-19 (the 2026-11-20 row is blank)
- `16:23:24`   blank newest row at the ECB: CISS_ES    shows 0.005 on 2026-11-19 (the 2026-11-20 row is blank)
- `16:23:24`   numeric rows left out for their status: {}
## 5. The desk in words

- `16:23:24`   EA_SOV_GDP   Euro area SovCISS, GDP-weighted                                   0.01123  2026-11-19  current  1w -0.00971  1m -0.02257  12m 0.00993  | lower than 76.5% of its history | high 0.75141 on 2011-11-29
- `16:23:24`   EA_SOV_EQ    Euro area SovCISS, equal-weighted                                 0.00694  2026-11-19  current  1w -0.00753  1m -0.0164  12m 0.00578  | lower than 85.5% of its history | high 0.77948 on 2011-11-29
- `16:23:24`   EA_SOV_RANGE Highest minus lowest country SovCISS                              0.17015  2026-11-19  current  1w -0.12284  1m -0.06693  12m 0.09414  | lower than 59.9% of its history | high 0.91329 on 2015-07-06
- `16:23:24`   EA_CURVE_10Y All euro-area governments minus AAA governments, 10-year         59.81 bp  2026-11-19  current  1w 1.37  1m -2.77  12m 18.23  | lower than 44.2% of its history | high 194.81 on 2012-07-23
- `16:23:24`   EA_CURVE_2Y  All euro-area governments minus AAA governments, 2-year          16.78 bp  2026-11-19  current  1w -0.09  1m -4.33  12m 8.34  | lower than 62.3% of its history | high 285.63 on 2011-11-25
- `16:23:24`   EA_ALL_10Y   Euro-area government 10-year spot rate, all issuers               4.0396%  2026-11-19  current  1w 0.088  1m -0.0553  12m 0.8386  | lower than 16.5% of its history | high 5.0686 on 2011-11-25
- `16:23:24`   EA_AAA_10Y   Euro-area government 10-year spot rate, AAA issuers               3.4415%  2026-11-19  current  1w 0.0743  1m -0.0276  12m 0.6563  | lower than 24.1% of its history | high 4.7763 on 2008-06-16
- `16:23:24`   Y10_U2       Euro area 10-year government yield                                4.0771%  2026-10-31  current  1w None  1m 0.1487  12m 1.0118  | lower than 45.7% of its history | high 10.1583 on 1992-08-31
- `16:23:24`   EA_TGT_DEBT  TARGET liabilities of the national central banks in deficit    1307.451 bn  2026-09-30  current  1w None  1m -1.547  12m 20.746  | lower than 13.6% of its history | high 1592.418 on 2022-09-30
- `16:23:24`   EA_CISS      Euro area CISS (systemic stress)                                  0.00946  2026-11-19  current  1w 0.0028  1m -0.00038  12m -0.02343  | lower than 86.5% of its history | high 0.94088 on 2008-11-20
- `16:23:24`   EA_CON_BMN   Bond market contribution to the euro-area CISS                    0.03358  2026-11-19  current  1w 0.00315  1m -0.00202  12m 0.01964  | lower than 88.5% of its history | high 0.25691 on 2008-12-17
- `16:23:24`   EA_CON_EMN   Equity market contribution to the euro-area CISS                   0.0303  2026-11-19  current  1w 0.01152  1m 0.01218  12m 0.0081  | lower than 71.1% of its history | high 0.17747 on 2008-11-21
- `16:23:24`   EA_CON_FIN   Financial intermediaries contribution to the euro-area CISS       0.03364  2026-11-19  current  1w 0.00474  1m 0.00187  12m 0.00628  | lower than 80.9% of its history | high 0.19418 on 2008-11-20
- `16:23:24`   EA_CON_FXN   Foreign exchange contribution to the euro-area CISS               0.00551  2026-11-19  current  1w -0.00409  1m 0.00072  12m 0.00087  | lower than 94.1% of its history | high 0.19332 on 2008-11-20
- `16:23:24`   EA_CON_MMN   Money market contribution to the euro-area CISS                   0.01401  2026-11-19  current  1w -0.00082  1m 0.00084  12m 0.00663  | lower than 87.9% of its history | high 0.127 on 2008-10-16
- `16:23:24`   EA_CON_CON   Correlation between the markets contribution to the euro-area    -0.10758  2026-11-19  current  1w -0.0117  1m -0.01397  12m -0.06495  | lower than 21.4% of its history | high -0.00098 on 2008-12-23
- `16:23:24`   cross-section 2026-11-19: FR 0.17391 BE 0.16125 GR 0.10067 IT 0.0474 AT 0.04222 DE 0.03026 IE 0.02415 NL 0.01731 FI 0.00602 ES 0.00567 PT 0.00377 | median 0.03026, range 0.17015
- `16:23:24`        country        euro   SovCISS    pct       10y    spread  TARGET bn       debt    CLIFS     CISS
- `16:23:24`   AT   Austria        yes    0.04222   42.1   3.6898%  27.38 bp -127.858 bn 83.58% GDP   0.0547  0.00524
- `16:23:24`   BE   Belgium        yes    0.16125   75.0     4.38%   96.4 bp 139.032 bn 110.18% GDP   0.0724  0.05922
- `16:23:24`   BG   Bulgaria       yes          -      -     4.09%   67.4 bp  21.348 bn 28.75% GDP   0.0758        -
- `16:23:24`   CN   China          no           -      -         -         -          -          -        -  0.01701
- `16:23:24`   CY   Cyprus         yes          -      -     4.04%   62.4 bp   13.26 bn 48.52% GDP   0.0549        -
- `16:23:24`   CZ   Czechia        no           -      -     5.37%  195.4 bp          - 43.83% GDP   0.0965        -
- `16:23:24`   DE   Germany        yes    0.03026   38.5    3.416%         - 1004.296 bn 63.43% GDP   0.0637  0.00798
- `16:23:24`   DK   Denmark        no           -      -    3.228%  -18.8 bp          - 27.09% GDP   0.0847        -
- `16:23:24`   EE   Estonia        yes          -      -     4.27%   85.4 bp   1.594 bn 26.15% GDP        -        -
- `16:23:24`   ES   Spain          yes    0.00567    4.4    4.066%   65.0 bp -453.798 bn 101.41% GDP    0.063    0.005
- `16:23:24`   FI   Finland        yes    0.00602    4.1    3.821%   40.5 bp  13.107 bn 90.25% GDP   0.1192   0.0087
- `16:23:24`   FR   France         yes    0.17391   75.7     4.89%  147.4 bp -183.45 bn 119.07% GDP   0.1002  0.08947
- `16:23:24`   GB   United Kingdom no           -      -         -         -          -          -    0.076  0.01996
- `16:23:24`   GR   Greece         yes    0.10067   61.0     4.41%   99.4 bp -102.861 bn 140.76% GDP    0.099        -
- `16:23:24`   HR   Croatia        yes          -      -     3.59%   17.4 bp  -9.569 bn 56.19% GDP   0.1003        -
- `16:23:24`   HU   Hungary        no           -      -     5.57%  215.4 bp          - 75.58% GDP   0.0778        -
- `16:23:24`   IE   Ireland        yes    0.02415   33.3   3.5525%  13.65 bp  93.687 bn 35.26% GDP   0.0733  0.00726
- `16:23:24`   IT   Italy          yes     0.0474   42.7     4.68%  126.4 bp -324.189 bn 139.86% GDP   0.1106  0.00948
- `16:23:24`   LT   Lithuania      yes          -      -     2.88%  -53.6 bp  16.806 bn 43.76% GDP   0.1238        -
- `16:23:24`   LU   Luxembourg     yes          -      -    3.695%   27.9 bp 201.956 bn 28.17% GDP   0.0743        -
- `16:23:24`   LV   Latvia         yes          -      -     4.28%   86.4 bp  -5.908 bn  48.0% GDP   0.0439        -
- `16:23:24`   MT   Malta          yes          -      -     4.39%   97.4 bp   3.385 bn  47.0% GDP   0.1194        -
- `16:23:24`   NL   Netherlands    yes    0.01731   20.3    3.535%   11.9 bp  53.243 bn 43.72% GDP    0.055  0.00485
- `16:23:24`   PL   Poland         no           -      -     6.32%  290.4 bp          - 64.48% GDP   0.0948        -
- `16:23:24`   PT   Portugal       yes    0.00377    1.6    3.917%   50.1 bp -94.612 bn 92.51% GDP   0.1265  0.00534
- `16:23:24`   RO   Romania        no           -      -     6.77%  335.4 bp          - 59.41% GDP   0.1749        -
- `16:23:24`   SE   Sweden         no           -      -     3.17%  -24.6 bp          - 35.31% GDP   0.1294        -
- `16:23:24`   SI   Slovenia       yes          -      -     3.99%   57.4 bp  12.942 bn 64.69% GDP    0.112        -
- `16:23:24`   SK   Slovakia       yes          -      -     4.31%   89.4 bp  -5.207 bn 60.58% GDP   0.1169        -
- `16:23:24`   US   United States  no           -      -         -         -          -          -        -  0.01452
- `16:23:24`   ended SovCISS SOVM_CZ  Czechia SovCISS (monthly series, e last 0.01717 on 2025-04-30 | history from 2000-09-30 | record 0.78307 on 2009-03-31 | 2011-12 peak [0.4078, '2011-12-31']
- `16:23:24`   ended SovCISS SOVM_DK  Denmark SovCISS (monthly series, e last 0.32256 on 2025-04-30 | history from 2002-11-30 | record 0.8067 on 2008-10-31 | 2011-12 peak [0.60962, '2011-11-30']
- `16:23:24`   ended SovCISS SOVM_GB  United Kingdom SovCISS (monthly se last 0.08951 on 2025-04-30 | history from 2002-11-30 | record 0.79698 on 2022-10-31 | 2011-12 peak [0.21522, '2011-09-30']
- `16:23:24`   ended SovCISS SOVM_HU  Hungary SovCISS (monthly series, e last 0.08498 on 2025-04-30 | history from 2000-09-30 | record 0.82476 on 2009-03-31 | 2011-12 peak [0.43887, '2011-12-31']
- `16:23:24`   ended SovCISS SOVM_PL  Poland SovCISS (monthly series, en last 0.17926 on 2025-04-30 | history from 2002-11-30 | record 0.59372 on 2023-01-31 | 2011-12 peak [0.11621, '2011-09-30']
- `16:23:24`   ended SovCISS SOVM_SE  Sweden SovCISS (monthly series, en last 0.12341 on 2025-04-30 | history from 2002-11-30 | record 0.82579 on 2022-07-31 | 2011-12 peak [0.68102, '2011-11-30']
- `16:23:24`   fiscal (four-quarter, % of GDP): AT balance -3.58% GDP, before interest -1.94% GDP, interest 1.64% GDP (2026-Q2) | BE balance -5.33% GDP, before interest -2.93% GDP, interest 2.4% GDP (2026-Q2) | BG balance -5.46% GDP, before interest -4.69% GDP, interest 0.77% GDP (2026-Q2) | CY balance 3.6% GDP, before interest 4.7% GDP, interest 1.1% GDP (2026-Q2) | CZ balance -2.64% GDP, before interest -1.31% GDP, interest 1.33% GDP (2026-Q2) | DE balance -3.7% GDP, before interest -2.58% GDP, interest 1.12% GDP (2026-Q2) | DK balance 2.88% GDP, before interest 3.63% GDP, interest 0.75% GDP (2026-Q2) | EE balance -2.11% GDP, before interest -1.62% GDP, interest 0.49% GDP (2026-Q2) | ES balance -2.53% GDP, before interest -0.08% GDP, interest 2.45% GDP (2026-Q2) | FI balance -3.73% GDP, before interest -2.01% GDP, interest 1.72% GDP (2026-Q2) | FR balance -5.11% GDP, before interest -2.7% GDP, interest 2.41% GDP (2026-Q2) | GR balance 2.13% GDP, before interest 5.27% GDP, interest 3.14% GDP (2026-Q2) | HR balance -3.48% GDP, before interest -2.1% GDP, interest 1.38% GDP (2026-Q2) | HU balance -6.38% GDP, before interest -2.65% GDP, interest 3.73% GDP (2026-Q2) | IE balance 2.03% GDP, before interest 2.53% GDP, interest 0.5% GDP (2026-Q2) | IT balance -2.98% GDP, before interest 0.99% GDP, interest 3.97% GDP (2026-Q2) | LT balance -1.71% GDP, before interest -0.68% GDP, interest 1.04% GDP (2026-Q2) | LU balance -1.31% GDP, before interest -0.91% GDP, interest 0.4% GDP (2026-Q2) | LV balance -2.25% GDP, before interest -1.11% GDP, interest 1.14% GDP (2026-Q2) | MT balance -3.5% GDP, before interest -2.25% GDP, interest 1.24% GDP (2026-Q2) | NL balance -1.88% GDP, before interest -1.12% GDP, interest 0.76% GDP (2026-Q2) | PL balance -6.7% GDP, before interest -4.12% GDP, interest 2.58% GDP (2026-Q2) | PT balance 0.49%
- `16:23:24`   CISS outside the euro area: United States CISS 0.01452 on 2026-11-19, history from 1980-01-02, 1987 window [0.71944, '1987-11-09'], Lehman window [0.89665, '2008-11-20'] | China CISS 0.01701 on 2026-11-13, history from 2006-10-10, 1987 window None, Lehman window [0.65061, '2008-12-12'] | United Kingdom CISS 0.01996 on 2026-11-19, history from 1980-01-02, 1987 window [0.43146, '1987-11-05'], Lehman window [0.773, '2008-12-03']
- `16:23:24`   auxiliaries: EUR/USD reference rate 1.099 on 2026-11-20 (exact 1.099) | Claims on euro-area residents in foreign currency 20.193 on 2026-11-13 (exact 20193) | Claims on non-euro-area residents in foreign currency 521.892 on 2026-11-13 (exact 521892)
- `16:23:24`   crisis peaks (highest reading inside each window; TARGET shows the lowest):
- `16:23:24`                                  EA_SOV_GDP   EA_SOV_RANGE SOV_GR       SOV_IT       SOV_ES       SOV_FR       EA_CURVE_10Y S10_GR       S10_IT       S10_FR       TGT_IT       TGT_DE      
- `16:23:24`     October 1987 crash           -            -            -            -            -            -            -            -            -            -            -            -           
- `16:23:24`     ERM crisis                   -            -            -            -            -            -            -            1784.0       694.9        104.0        -            -           
- `16:23:24`     Russia default and LTCM      -            -            -            -            -            -            -            439.0        46.6         14.0         -            -           
- `16:23:24`     11 September 2001            0.07386      0.1705       0.09043      0.07051      0.0435       0.17981      -            50.0         38.6         13.0         -            -           
- `16:23:24`     WorldCom and the 2002 lows   0.05052      0.24459      0.1386       0.08725      0.08868      0.08636      -            36.0         29.7         9.0          -            -           
- `16:23:24`     Lehman and the global crisis 0.46418      0.74082      0.12877      0.33474      0.50295      0.68914      41.2         285.0        155.1        63.0         22.919       70.196      
- `16:23:24`     Greece asks for a rescue     0.46533      0.63005      0.72908      0.73184      0.83849      0.38758      79.57        656.0        156.2        53.0         21.839       213.678     
- `16:23:24`     Ireland rescue               0.52448      0.83342      0.59918      0.67299      0.86049      0.55692      94.96        910.0        169.3        47.0         3.41         299.445     
- `16:23:24`     Italy and Spain drawn in     0.75141      0.88021      0.96687      0.99032      0.95198      0.75312      188.31       1921.0       518.7        154.0        -191.379     343.662     
- `16:23:24`     Spain's banks, "whatever it  0.69866      0.76302      0.95324      0.97223      0.9919       0.76346      194.81       2652.0       475.6        141.0        -280.093     698.567     
- `16:23:24`     Cyprus bail-in               0.29282      0.78995      0.77977      0.84387      0.77311      0.28497      139.17       1003.0       329.0        72.0         -242.939     588.722     
- `16:23:24`     Greek referendum             0.12322      0.91329      0.92855      0.34859      0.46908      0.08948      78.17        1064.0       140.9        41.0         -188.63      531.074     
- `16:23:24`     China devaluation and equity 0.07915      0.79557      0.80336      0.17976      0.27901      0.14186      71.43        1024.0       138.8        42.0         -251.264     555.175     
- `16:23:24`     Brexit vote                  0.15018      0.71977      0.75914      0.28312      0.21517      0.07516      74.87        794.0        147.3        41.0         -288.873     681.071     
- `16:23:24`     Italy: government and budget 0.12344      0.8658       0.24833      0.86955      0.14879      0.0526       93.97        411.0        308.0        45.0         -492.526     912.448     
- `16:23:24`     COVID                        0.46798      0.75367      0.24581      0.81446      0.59179      0.44506      104.86       251.0        224.9        51.0         -512.899     821.563     
- `16:23:24`     ECB lift-off and the TPI     0.31647      0.69503      0.11877      0.48006      0.13832      0.322        85.87        248.4        227.7        62.8         -640.085     1166.155    
- `16:23:24`     UK gilt crisis               0.44715      0.79836      0.10753      0.64451      0.16049      0.54489      87.37        268.4        234.6        61.4         -714.932     1230.005    
- `16:23:24`     SVB and Credit Suisse        0.24292      0.53359      0.11262      0.09331      0.12907      0.23151      77.04        190.7        185.7        53.7         -685.235     1170.62     
- `16:23:24`     French snap election         0.0574       0.25191      0.0433       0.07307      0.04663      0.26546      69.61        116.6        145.8        66.6         -485.288     1090.444    
- `16:23:24`     French budget, government fa 0.04643      0.19191      0.0195       0.07042      0.0326       0.19815      59.95        88.5         126.4        78.5         -428.623     1066.511    
- `16:23:24`     US tariff shock              0.06027      0.32678      0.03         0.04634      0.03055      0.21709      59.8         92.9         120.1        74.9         -371.121     1075.272    
- `16:23:24`     highest readings of EA_SOV_GDP by the data: 0.75141 on 2011-11-29  0.69866 on 2012-07-11  0.55466 on 2011-08-01  0.52448 on 2010-12-09  0.46798 on 2020-03-31  0.46533 on 2010-06-07  0.46418 on 2009-03-12  0.44715 on 2022-10-28
- `16:23:24`     highest readings of SOV_FR by the data: 0.79086 on 2012-01-18  0.76346 on 2012-07-31  0.68914 on 2008-12-19  0.67342 on 2008-03-12  0.55692 on 2010-12-09  0.54489 on 2022-10-19  0.5319 on 2011-08-17  0.4608 on 2000-06-02
- `16:23:24`     highest readings of EA_CURVE_10Y by the data: 194.81 on 2012-07-23  188.31 on 2011-11-25  142.91 on 2012-12-10  131.85 on 2011-07-28  130.03 on 2013-04-09  107.43 on 2013-08-07  104.86 on 2020-04-22  94.96 on 2010-11-30
## 6. For the page's tests

- `16:23:25` ✅ the document compiles from live originals and seven ECB responses, agrees with the warehouse, and replays; nothing was written
