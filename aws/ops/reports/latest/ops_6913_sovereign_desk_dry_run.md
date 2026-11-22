# ops 6913 - DRY RUN: the sovereign-stress desk document, compiled from live originals (nothing written), 2026-11-22T15:06+00:00

**Status:** success  
**Duration:** 57.9s  
**Finished:** 2026-11-22T15:07:48+00:00  

## Data

| checked_against_warehouse | countries | differ | document_gzip | failed_checks | history_gzip | measures | seconds |
|---|---|---|---|---|---|---|---|
| 58 | 30 | 0 | 119228 | 0 | 371800 | 176 | 57 |

## Log
## 1. Compile, as the Lambda will

- `15:06:54`   ECB YC/B.U2.EUR.4F.G_N_A+G_N_C.SV_C_YM.SR_2Y+SR_10Y?format=csvdata -> 12767481 bytes in 2.5s
- `15:06:58`   ECB IRS/M..L.L40.CI.0000..N.Z?format=csvdata -> 5689197 bytes in 2.4s
- `15:07:10`   ECB TGB/M...A094T.U2.EUR.E?format=csvdata -> 891532 bytes in 10.5s
- `15:07:14`   ECB GFS/Q.N..W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T?format=csv -> 2928190 bytes in 3.4s
- `15:07:19`   ECB EXR/D.USD.EUR.SP00.A?format=csvdata -> 1492522 bytes in 3.9s
- `15:07:23`   ECB ILM/W.U2.C.A030000.U2.Z06?format=csvdata -> 540155 bytes in 2.9s
- `15:07:27`   ECB ILM/W.U2.C.A020000.U4.Z06?format=csvdata -> 528477 bytes in 2.8s
- `15:07:47` published (to the overlay) in 57s: {"published": true, "generated_at": "2026-11-22T15:07:27.926214+00:00", "coverage": {"measures": 176, "countries": 30, "errors": 0, "status": "measured"}, "replay": {"manifest_key": "data/ciss-research/sovereign/runs/964bb25b385269987b42ae8bb17b8142a20a1e04fce3561d6a6cfeb43172fdaf.json", "output_sha
- `15:07:47` document 543437 bytes (119228 gzip) | history 2130284 bytes (371800 gzip) | 62 objects read from the bucket | writes held back: 11
- `15:07:47` what a live run would write: {"evidence": 7, "compiler": 1, "run receipt": 1, "data/ciss-sovereign-history.json": 1, "data/ciss-sovereign.json": 1}
- `15:07:47` errors: {} | coverage {"countries": 30, "errors": 0, "measures": 176, "status": "measured"} | ended at source: 4
- `15:07:47`     ended: CLIFS_EE   CLIFS.M.EE._Z.4F.EC.CLIFS_CI.IDX last 2010-12-31
- `15:07:47`     ended: DEBT_GB    GFS.Q.N.GB.W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T last 2024-06-30
- `15:07:47`     ended: DEBT_US    GFS.Q.N.US.W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T last 2024-06-30
- `15:07:47`     ended: Y10_GB     IRS.M.GB.L.L40.CI.0000.GBP.N.Z last 2020-01-31
## 2. The sources

- `15:07:48`   ciss             CISS, SovCISS and CLIFS (the warehouse) | 61 series, - rows, newest -
- `15:07:48`   curve            Euro-area government yield curve, AAA issuers and all issuers (daily) | 4 series, 22712 rows, newest 2026-11-19
- `15:07:48`   yields           10-year government yields by country, convergence statistic (monthly) | 34 series, 12175 rows, newest 2026-10
- `15:07:48`   target           TARGET balances of the national central banks, end of month | 23 series, 4871 rows, newest 2026-09
- `15:07:48`   debt             General government debt, percent of GDP (quarterly) | 34 series, 3998 rows, newest 2026-Q2
- `15:07:48`   eurusd           Euro reference rate against the US dollar (daily) | 1 series, 7203 rows, newest 2026-11-20
- `15:07:48`   fx_claims_ea     Eurosystem claims on euro-area residents in foreign currency (weekly) | 1 series, 1455 rows, newest 2026-W46
- `15:07:48`   fx_claims_nonea  Eurosystem claims on non-euro-area residents in foreign currency (weekly) | 1 series, 1455 rows, newest 2026-W46
## 3. The measures, by group

- `15:07:48`   sovciss        11 measures ({"current": 11}) | newest 2026-11-19, oldest 2026-11-19
- `15:07:48`   euro_area       7 measures ({"current": 7}) | newest 2026-11-19, oldest 2026-09-30
- `15:07:48`   contribution    6 measures ({"current": 6}) | newest 2026-11-19, oldest 2026-11-19
- `15:07:48`   ciss           13 measures ({"current": 13}) | newest 2026-11-19, oldest 2026-11-13
- `15:07:48`   clifs          27 measures ({"current": 27}) | newest 2026-09-30, oldest 2026-09-30
- `15:07:48`   curve           4 measures ({"current": 4}) | newest 2026-11-19, oldest 2026-11-19
- `15:07:48`   yield          28 measures ({"current": 28}) | newest 2026-10-31, oldest 2026-10-31
- `15:07:48`   spread         27 measures ({"current": 27}) | newest 2026-10-31, oldest 2026-10-31
- `15:07:48`   target         22 measures ({"current": 22}) | newest 2026-09-30, oldest 2026-09-30
- `15:07:48`   debt           28 measures ({"current": 28}) | newest 2026-06-30, oldest 2026-06-30
- `15:07:48`   auxiliary       3 measures ({"current": 3}) | newest 2026-11-20, oldest 2026-11-13
## 4. Against the warehouse's own rows

- `15:07:48`   58 measures checked against their warehouse rows, 0 differ
- `15:07:48`   blank newest row at the ECB: CISS_DE    shows 0.00798 on 2026-11-19 (the 2026-11-20 row is blank)
- `15:07:48`   blank newest row at the ECB: CISS_ES    shows 0.005 on 2026-11-19 (the 2026-11-20 row is blank)
- `15:07:48`   numeric rows left out for their status: {}
## 5. The desk in words

- `15:07:48`   EA_SOV_GDP   Euro area SovCISS, GDP-weighted                                   0.01123  2026-11-19  current  1w -0.00971  1m -0.02257  12m 0.00993  | lower than 76.5% of its history | high 0.75141 on 2011-11-29
- `15:07:48`   EA_SOV_EQ    Euro area SovCISS, equal-weighted                                 0.00694  2026-11-19  current  1w -0.00753  1m -0.0164  12m 0.00578  | lower than 85.5% of its history | high 0.77948 on 2011-11-29
- `15:07:48`   EA_SOV_RANGE Highest minus lowest country SovCISS                              0.17015  2026-11-19  current  1w -0.12284  1m -0.06693  12m 0.09414  | lower than 59.9% of its history | high 0.91329 on 2015-07-06
- `15:07:48`   EA_CURVE_10Y All euro-area governments minus AAA governments, 10-year         59.81 bp  2026-11-19  current  1w 1.37  1m -2.77  12m 18.23  | lower than 44.2% of its history | high 194.81 on 2012-07-23
- `15:07:48`   EA_CURVE_2Y  All euro-area governments minus AAA governments, 2-year          16.78 bp  2026-11-19  current  1w -0.09  1m -4.33  12m 8.34  | lower than 62.3% of its history | high 285.63 on 2011-11-25
- `15:07:48`   EA_ALL_10Y   Euro-area government 10-year spot rate, all issuers               4.0396%  2026-11-19  current  1w 0.088  1m -0.0553  12m 0.8386  | lower than 16.5% of its history | high 5.0686 on 2011-11-25
- `15:07:48`   EA_AAA_10Y   Euro-area government 10-year spot rate, AAA issuers               3.4415%  2026-11-19  current  1w 0.0743  1m -0.0276  12m 0.6563  | lower than 24.1% of its history | high 4.7763 on 2008-06-16
- `15:07:48`   Y10_U2       Euro area 10-year government yield                                4.0771%  2026-10-31  current  1w None  1m 0.1487  12m 1.0118  | lower than 45.7% of its history | high 10.1583 on 1992-08-31
- `15:07:48`   EA_TGT_DEBT  TARGET liabilities of the national central banks in deficit    1307.451 bn  2026-09-30  current  1w None  1m -1.547  12m 20.746  | lower than 13.6% of its history | high 1592.418 on 2022-09-30
- `15:07:48`   EA_CISS      Euro area CISS (systemic stress)                                  0.00946  2026-11-19  current  1w 0.0028  1m -0.00038  12m -0.02343  | lower than 86.5% of its history | high 0.94088 on 2008-11-20
- `15:07:48`   EA_CON_BMN   Bond market contribution to the euro-area CISS                    0.03358  2026-11-19  current  1w 0.00315  1m -0.00202  12m 0.01964  | lower than 88.5% of its history | high 0.25691 on 2008-12-17
- `15:07:48`   EA_CON_EMN   Equity market contribution to the euro-area CISS                   0.0303  2026-11-19  current  1w 0.01152  1m 0.01218  12m 0.0081  | lower than 71.1% of its history | high 0.17747 on 2008-11-21
- `15:07:48`   EA_CON_FIN   Financial intermediaries contribution to the euro-area CISS       0.03364  2026-11-19  current  1w 0.00474  1m 0.00187  12m 0.00628  | lower than 80.9% of its history | high 0.19418 on 2008-11-20
- `15:07:48`   EA_CON_FXN   Foreign exchange contribution to the euro-area CISS               0.00551  2026-11-19  current  1w -0.00409  1m 0.00072  12m 0.00087  | lower than 94.1% of its history | high 0.19332 on 2008-11-20
- `15:07:48`   EA_CON_MMN   Money market contribution to the euro-area CISS                   0.01401  2026-11-19  current  1w -0.00082  1m 0.00084  12m 0.00663  | lower than 87.9% of its history | high 0.127 on 2008-10-16
- `15:07:48`   EA_CON_CON   Correlation between the markets contribution to the euro-area    -0.10758  2026-11-19  current  1w -0.0117  1m -0.01397  12m -0.06495  | lower than 21.4% of its history | high -0.00098 on 2008-12-23
- `15:07:48`   cross-section 2026-11-19: FR 0.17391 BE 0.16125 GR 0.10067 IT 0.0474 AT 0.04222 DE 0.03026 IE 0.02415 NL 0.01731 FI 0.00602 ES 0.00567 PT 0.00377 | median 0.03026, range 0.17015
- `15:07:48`        country        euro   SovCISS    pct       10y    spread  TARGET bn       debt    CLIFS     CISS
- `15:07:48`   AT   Austria        yes    0.04222   42.1   3.6898%  27.38 bp -127.858 bn 83.58% GDP   0.0547  0.00524
- `15:07:48`   BE   Belgium        yes    0.16125   75.0     4.38%   96.4 bp 139.032 bn 110.18% GDP   0.0724  0.05922
- `15:07:48`   BG   Bulgaria       yes          -      -     4.09%   67.4 bp  21.348 bn 28.75% GDP   0.0758        -
- `15:07:48`   CN   China          no           -      -         -         -          -          -        -  0.01701
- `15:07:48`   CY   Cyprus         yes          -      -     4.04%   62.4 bp   13.26 bn 48.52% GDP   0.0549        -
- `15:07:48`   CZ   Czechia        no           -      -     5.37%  195.4 bp          - 43.83% GDP   0.0965        -
- `15:07:48`   DE   Germany        yes    0.03026   38.5    3.416%         - 1004.296 bn 63.43% GDP   0.0637  0.00798
- `15:07:48`   DK   Denmark        no           -      -    3.228%  -18.8 bp          - 27.09% GDP   0.0847        -
- `15:07:48`   EE   Estonia        yes          -      -     4.27%   85.4 bp   1.594 bn 26.15% GDP        -        -
- `15:07:48`   ES   Spain          yes    0.00567    4.4    4.066%   65.0 bp -453.798 bn 101.41% GDP    0.063    0.005
- `15:07:48`   FI   Finland        yes    0.00602    4.1    3.821%   40.5 bp  13.107 bn 90.25% GDP   0.1192   0.0087
- `15:07:48`   FR   France         yes    0.17391   75.7     4.89%  147.4 bp -183.45 bn 119.07% GDP   0.1002  0.08947
- `15:07:48`   GB   United Kingdom no           -      -         -         -          -          -    0.076  0.01996
- `15:07:48`   GR   Greece         yes    0.10067   61.0     4.41%   99.4 bp -102.861 bn 140.76% GDP    0.099        -
- `15:07:48`   HR   Croatia        yes          -      -     3.59%   17.4 bp  -9.569 bn 56.19% GDP   0.1003        -
- `15:07:48`   HU   Hungary        no           -      -     5.57%  215.4 bp          - 75.58% GDP   0.0778        -
- `15:07:48`   IE   Ireland        yes    0.02415   33.3   3.5525%  13.65 bp  93.687 bn 35.26% GDP   0.0733  0.00726
- `15:07:48`   IT   Italy          yes     0.0474   42.7     4.68%  126.4 bp -324.189 bn 139.86% GDP   0.1106  0.00948
- `15:07:48`   LT   Lithuania      yes          -      -     2.88%  -53.6 bp  16.806 bn 43.76% GDP   0.1238        -
- `15:07:48`   LU   Luxembourg     yes          -      -    3.695%   27.9 bp 201.956 bn 28.17% GDP   0.0743        -
- `15:07:48`   LV   Latvia         yes          -      -     4.28%   86.4 bp  -5.908 bn  48.0% GDP   0.0439        -
- `15:07:48`   MT   Malta          yes          -      -     4.39%   97.4 bp   3.385 bn  47.0% GDP   0.1194        -
- `15:07:48`   NL   Netherlands    yes    0.01731   20.3    3.535%   11.9 bp  53.243 bn 43.72% GDP    0.055  0.00485
- `15:07:48`   PL   Poland         no           -      -     6.32%  290.4 bp          - 64.48% GDP   0.0948        -
- `15:07:48`   PT   Portugal       yes    0.00377    1.6    3.917%   50.1 bp -94.612 bn 92.51% GDP   0.1265  0.00534
- `15:07:48`   RO   Romania        no           -      -     6.77%  335.4 bp          - 59.41% GDP   0.1749        -
- `15:07:48`   SE   Sweden         no           -      -     3.17%  -24.6 bp          - 35.31% GDP   0.1294        -
- `15:07:48`   SI   Slovenia       yes          -      -     3.99%   57.4 bp  12.942 bn 64.69% GDP    0.112        -
- `15:07:48`   SK   Slovakia       yes          -      -     4.31%   89.4 bp  -5.207 bn 60.58% GDP   0.1169        -
- `15:07:48`   US   United States  no           -      -         -         -          -          -        -  0.01452
- `15:07:48`   auxiliaries: EUR/USD reference rate 1.099 on 2026-11-20 (exact 1.099) | Claims on euro-area residents in foreign currency 20.193 on 2026-11-13 (exact 20193) | Claims on non-euro-area residents in foreign currency 521.892 on 2026-11-13 (exact 521892)
- `15:07:48`   crisis peaks (highest reading inside each window; TARGET shows the lowest):
- `15:07:48`                                  EA_SOV_GDP   EA_SOV_RANGE SOV_GR       SOV_IT       SOV_ES       SOV_FR       EA_CURVE_10Y S10_GR       S10_IT       S10_FR       TGT_IT       TGT_DE      
- `15:07:48`     Lehman and the global crisis 0.46418      0.74082      0.12877      0.33474      0.50295      0.68914      41.2         285.0        155.1        63.0         22.919       70.196      
- `15:07:48`     Greece asks for a rescue     0.46533      0.63005      0.72908      0.73184      0.83849      0.38758      79.57        656.0        156.2        53.0         21.839       213.678     
- `15:07:48`     Ireland rescue               0.52448      0.83342      0.59918      0.67299      0.86049      0.55692      94.96        910.0        169.3        47.0         3.41         299.445     
- `15:07:48`     Italy and Spain drawn in     0.75141      0.88021      0.96687      0.99032      0.95198      0.75312      188.31       1921.0       518.7        154.0        -191.379     343.662     
- `15:07:48`     Spain's banks, "whatever it  0.69866      0.76302      0.95324      0.97223      0.9919       0.76346      194.81       2652.0       475.6        141.0        -280.093     698.567     
- `15:07:48`     Cyprus bail-in               0.29282      0.78995      0.77977      0.84387      0.77311      0.28497      139.17       1003.0       329.0        72.0         -242.939     588.722     
- `15:07:48`     Greek referendum             0.12322      0.91329      0.92855      0.34859      0.46908      0.08948      78.17        1064.0       140.9        41.0         -188.63      531.074     
- `15:07:48`     Brexit vote                  0.15018      0.71977      0.75914      0.28312      0.21517      0.07516      74.87        794.0        147.3        41.0         -288.873     681.071     
- `15:07:48`     Italy: government and budget 0.12344      0.8658       0.24833      0.86955      0.14879      0.0526       93.97        411.0        308.0        45.0         -492.526     912.448     
- `15:07:48`     COVID                        0.46798      0.75367      0.24581      0.81446      0.59179      0.44506      104.86       251.0        224.9        51.0         -512.899     821.563     
- `15:07:48`     ECB lift-off and the TPI     0.31647      0.69503      0.11877      0.48006      0.13832      0.322        85.87        248.4        227.7        62.8         -640.085     1166.155    
- `15:07:48`     UK gilt crisis               0.44715      0.79836      0.10753      0.64451      0.16049      0.54489      87.37        268.4        234.6        61.4         -714.932     1230.005    
- `15:07:48`     SVB and Credit Suisse        0.24292      0.53359      0.11262      0.09331      0.12907      0.23151      77.04        190.7        185.7        53.7         -685.235     1170.62     
- `15:07:48`     French snap election         0.0574       0.25191      0.0433       0.07307      0.04663      0.26546      69.61        116.6        145.8        66.6         -485.288     1090.444    
- `15:07:48`     French budget, government fa 0.04643      0.19191      0.0195       0.07042      0.0326       0.19815      59.95        88.5         126.4        78.5         -428.623     1066.511    
- `15:07:48`     US tariff shock              0.06027      0.32678      0.03         0.04634      0.03055      0.21709      59.8         92.9         120.1        74.9         -371.121     1075.272    
- `15:07:48`     highest readings of EA_SOV_GDP by the data: 0.75141 on 2011-11-29  0.69866 on 2012-07-11  0.55466 on 2011-08-01  0.52448 on 2010-12-09  0.46798 on 2020-03-31  0.46533 on 2010-06-07  0.46418 on 2009-03-12  0.44715 on 2022-10-28
- `15:07:48`     highest readings of SOV_FR by the data: 0.79086 on 2012-01-18  0.76346 on 2012-07-31  0.68914 on 2008-12-19  0.67342 on 2008-03-12  0.55692 on 2010-12-09  0.54489 on 2022-10-19  0.5319 on 2011-08-17  0.4608 on 2000-06-02
- `15:07:48`     highest readings of EA_CURVE_10Y by the data: 194.81 on 2012-07-23  188.31 on 2011-11-25  142.91 on 2012-12-10  131.85 on 2011-07-28  130.03 on 2013-04-09  107.43 on 2013-08-07  104.86 on 2020-04-22  94.96 on 2010-11-30
## 6. For the page's tests

- `15:07:48` ✅ the document compiles from live originals and seven ECB responses, agrees with the warehouse, and replays; nothing was written
