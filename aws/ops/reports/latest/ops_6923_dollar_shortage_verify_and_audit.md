# ops 6923 - the foreign-currency claims verified at the ECB; the site's other dollar measures (read-only), 2026-11-22T21:23+00:00

**Status:** success  
**Duration:** 321.9s  
**Finished:** 2026-11-22T21:29:18+00:00  

## Data

| failed_checks | fred_resolved | fresh_ecb_series |
|---|---|---|
| 0 | 24 | 8 |

## Log
## A. The Eurosystem's foreign-currency items, fresh from the ECB

- `21:24:02` ILM/W.U2.C..U4.Z06 -> 1131368 bytes, 6 series
- `21:24:02`   ILM.W.U2.C.A020000.U4.Z06       1455 obs 1999-01-01 .. 2026-11-13 last    521.892 bn | unit EUR x10^6 | Euro area (changing composition), Eurosystem reporting sector - Claims on non-euro area residents denominated in foreign currency, All currencies exce
- `21:24:02`   ILM.W.U2.C.A020100.U4.Z06         47 obs 2025-11-07 .. 2026-11-13 last    233.386 bn | unit EUR x10^6 | Euro area (changing composition), Eurosystem reporting sector - Receivables from the IMF, All currencies except EUR - Extra Euro area counterpart
- `21:24:02`   ILM.W.U2.C.A020200.U4.Z06         47 obs 2025-11-07 .. 2026-11-13 last    288.507 bn | unit EUR x10^6 | Euro area (changing composition), Eurosystem reporting sector - Balances with banks and security investments, external loans, other external assets, A
- `21:24:02`   ILM.W.U2.C.L080000.U4.Z06       1455 obs 1999-01-01 .. 2026-11-13 last      0.430 bn | unit EUR x10^6 | Euro area (changing composition), Eurosystem reporting sector - Liabilities to non-euro area residents denominated in foreign currency, All currencies
- `21:24:02`   ILM.W.U2.C.L080100.U4.Z06         47 obs 2025-11-07 .. 2026-11-13 last      0.430 bn | unit EUR x10^6 | Euro area (changing composition), Eurosystem reporting sector - Deposits, balances and other liabilities, All currencies except EUR - Extra Euro area 
- `21:24:02`   ILM.W.U2.C.L080200.U4.Z06         47 obs 2025-11-07 .. 2026-11-13 last      0.000 bn | unit EUR x10^6 | Euro area (changing composition), Eurosystem reporting sector - Liabilities arising from the credit facility under ERM II, All currencies except EUR -
- `21:24:11` ILM/W.U2.C..U2.Z06 -> 1092993 bytes, 2 series
- `21:24:11`   ILM.W.U2.C.A030000.U2.Z06       1455 obs 1999-01-01 .. 2026-11-13 last     20.193 bn | unit EUR x10^6 | Euro area (changing composition), Eurosystem reporting sector - Claims on euro area residents denominated in foreign currency, All currencies except E
- `21:24:11`   ILM.W.U2.C.L070000.U2.Z06       1455 obs 1999-01-01 .. 2026-11-13 last     15.310 bn | unit EUR x10^6 | Euro area (changing composition), Eurosystem reporting sector - Liabilities to euro area residents denominated in foreign currency, All currencies exc
- `21:24:11` AUX_FX_NONEA (ILM.W.U2.C.A020000.U4.Z06): published 1455 observations (written 19:12), fresh 1455 | same date and value: 1455 | differ: 0 [] | only published: [] | only fresh: []
- `21:24:11` AUX_FX_EA (ILM.W.U2.C.A030000.U2.Z06): published 1455 observations (written 19:12), fresh 1455 | same date and value: 1455 | differ: 0 [] | only published: [] | only fresh: []
- `21:24:11` item 2 against 2.1 + 2.2 on 47 weeks (2025-11-07 .. 2026-11-13): largest difference 0.001 bn on 2026-01-02 | latest: 2 = 521.892, 2.1 receivables from the IMF = 233.386, 2.2 balances, securities, loans = 288.507
- `21:24:11`   week to 2026-01-02: item 2    +10.8 bn = IMF receivables     +2.2 + balances / securities / loans     +8.6
- `21:24:11`   week to 2026-04-03: item 2     +5.8 bn = IMF receivables     +3.1 + balances / securities / loans     +2.6
- `21:24:11`   week to 2026-07-03: item 2     +4.5 bn = IMF receivables     +1.6 + balances / securities / loans     +2.9
- `21:24:11`   week to 2026-03-27: item 2     +2.4 bn = IMF receivables     -0.2 + balances / securities / loans     +2.5
- `21:24:11`   week to 2026-04-17: item 2     -2.3 bn = IMF receivables     -0.1 + balances / securities / loans     -2.2
- `21:24:11`   week to 2026-05-15: item 2     +2.3 bn = IMF receivables     +0.2 + balances / securities / loans     +2.0
- `21:24:11`   week to 2026-05-29: item 2     +2.0 bn = IMF receivables     +0.3 + balances / securities / loans     +1.7
- `21:24:11`   week to 2026-06-05: item 2     -1.6 bn = IMF receivables     -0.1 + balances / securities / loans     -1.5
- `21:24:11`   week to 2026-11-13: item 2     -1.6 bn = IMF receivables     -0.0 + balances / securities / loans     -1.6
- `21:24:11`   week to 2026-05-08: item 2     +1.5 bn = IMF receivables     +0.2 + balances / securities / loans     +1.3
## B1. The site's own dollar documents

- `21:24:12` data/eurodollar-plumbing.json | 244691 bytes, written 2026-11-22 12:03 | 124 measurements, 34 naming a dollar-shortage word | top level: {38: ai, allocation_pct, call, calls_eligible, comparisons, composite_score, context_snapshots, contract, engine, errors}
- `21:24:12`     /comparisons/bill_ois                                          Overnight SOFR minus three-month bill quote    -25.0 basis_poin 2026-11-19
- `21:24:12`     /comparisons/cp_ois                                            Three-month financial CP minus overnight SOFR  33.0 basis_poin 2026-11-18
- `21:24:12`     /comparisons/cpn_ois                                           Three-month nonfinancial CP minus overnight SO None basis_poin 2026-11-05
- `21:24:12`     /comparisons/sofr99_iorb                                       SOFR 99th percentile minus IORB                8.0 basis_poin 2026-11-19
- `21:24:12`     /comparisons/sofr_iorb                                         SOFR minus IORB                                1.0 basis_poin 2026-11-19
- `21:24:12`     /layers/backstops/metrics[0]                                   Assets: Central Bank Liquidity Swaps: Central  146.0 Millions o 
- `21:24:12`     /layers/backstops/metrics[1]                                   Assets: Other: Repurchase Agreements - Foreign 1.0 Millions o 
- `21:24:12`     /layers/backstops/metrics[3]                                   Eurosystem foreign-currency claims on euro-are 20193.0 EUR_millio 
- `21:24:12`     /layers/bank_funding/metrics[2]                                3-Month Treasury Bill Secondary Market Rate, D 4.16 Percent 
- `21:24:12`     /layers/bank_funding/metrics[9]                                OFR FSI · United States                        -0.737 index_poin 
- `21:24:12`     /layers/bank_funding/metrics[12]                               Three-month financial CP minus overnight SOFR  33.0 basis_poin 
- `21:24:12`     /layers/bank_funding/metrics[13]                               Three-month nonfinancial CP minus overnight SO None basis_poin 
- `21:24:12`     /layers/bank_funding/metrics[14]                               Overnight SOFR minus three-month bill quote    -25.0 basis_poin 
- `21:24:12`     /layers/credit/metrics[0]                                      ICE BofA US High Yield Index Option-Adjusted S 3.52 Percent 
- `21:24:12`     /layers/credit/metrics[1]                                      ICE BofA US Corporate Index Option-Adjusted Sp 0.94 Percent 
- `21:24:12`     /layers/fx/metrics[0]                                          Nominal Broad U.S. Dollar Index                122.6503 Index Jan  
- `21:24:12`     /layers/fx/metrics[1]                                          Memorandum Items: Custody Holdings: Marketable 2544011.0 Millions o 
- `21:24:12`     /layers/fx/metrics[2]                                          Net Due to Related Foreign Offices, All Commer 412.1479 Billions o 
- `21:24:12`     /layers/us_core/metrics[5]                                     Overnight Reverse Repurchase Agreements: Treas 0.401 Billions o 
- `21:24:12`     /layers/us_core/metrics[21]                                    SOFR minus IORB                                1.0 basis_poin 
- `21:24:12`     /layers/us_core/metrics[22]                                    SOFR 99th percentile minus IORB                8.0 basis_poin 
- `21:24:12`     /measurements/BAMLC0A0CM                                       ICE BofA US Corporate Index Option-Adjusted Sp 0.94 Percent 2026-11-19
- `21:24:12` data/eurodollar-stress.json | 56262 bytes, written 2026-11-21 21:48 | 18 measurements, 10 naming a dollar-shortage word | top level: {37: as_of, call, calls_eligible, cold_signals, compatibility, composite_score, composite_stress_score, contract, dependency_graph, duration_s}
- `21:24:12`     /measurements/BAMLC0A0CM                                       ICE BofA US Corporate Index Option-Adjusted Sp 0.94 Percent 
- `21:24:12`     /measurements/BAMLH0A0HYM2                                     ICE BofA US High Yield Index Option-Adjusted S 3.52 Percent 
- `21:24:12`     /measurements/DGS10                                            Market Yield on U.S. Treasury Securities at 10 5.04 Percent 
- `21:24:12`     /measurements/DTB3                                             3-Month Treasury Bill Secondary Market Rate, D 4.16 Percent 
- `21:24:12`     /measurements/DTWEXBGS                                         Nominal Broad U.S. Dollar Index                122.6503 Index Jan  
- `21:24:12`     /measurements/SOFR                                             Secured Overnight Financing Rate               3.91 Percent 
- `21:24:12`     /signals[1]                                                    ICE BofA US High Yield Index Option-Adjusted S 3.52 Percent 2026-11-19
- `21:24:12`     /signals[2]                                                    ICE BofA US Corporate Index Option-Adjusted Sp 0.94 Percent 2026-11-19
- `21:24:12`     /signals[4]                                                    Nominal Broad U.S. Dollar Index                122.6503 Index Jan  2026-11-13
- `21:24:12`     /signals[5]                                                    3-Month Treasury Bill Secondary Market Rate, D 4.16 Percent 2026-11-19
- `21:24:12` data/usd-funding.json | 10290 bytes, written 2026-11-20 22:21 | 13 measurements, 5 naming a dollar-shortage word | top level: {13: version, engine, generated, reference_rates, spreads_vs_iorb, bank_wholesale_funding, commercial_paper, sofr_term_structure, bis_lbs_usd, not_entitled}
- `21:24:12`     /bank_wholesale_funding/net_due_foreign                        Net due to related foreign offices             412.1479 $bn 2026-11-11
- `21:24:12`     /sofr_term_structure/sofr_30d_avg                              30-day average SOFR                            3.92334  2026-11-20
- `21:24:12`     /sofr_term_structure/sofr_90d_avg                              90-day average SOFR                            3.84103  2026-11-20
- `21:24:12`     /sofr_term_structure/sofr_180d_avg                             180-day average SOFR                           3.75816  2026-11-20
- `21:24:12`     /sofr_term_structure/sofr_index                                SOFR Index (compounded)                        1.26784511  2026-11-20
- `21:24:12` data/dollar-radar.json | 1046245 bytes, written 2026-11-22 19:16 | 4000 measurements, 72 naming a dollar-shortage word | top level: {31: benchmark_identity, bilaterals, call, calls_eligible, canaries_dump, canaries_pump, context_series, contract, dependency_graph, derived}
- `21:24:12`     /series/DEXBZUS/comparisons/month/dollar_strength/baseline                                                    4.9965  2026-10-13
- `21:24:12`     /series/DEXBZUS/comparisons/month/dollar_strength/current                                                     5.6449  2026-11-13
- `21:24:12`     /series/DEXBZUS/comparisons/quarter/dollar_strength/baseline                                                  5.1879  2026-08-13
- `21:24:12`     /series/DEXBZUS/comparisons/quarter/dollar_strength/current                                                   5.6449  2026-11-13
- `21:24:12`     /series/DEXBZUS/comparisons/week/dollar_strength/baseline                                                     5.4895  2026-11-06
- `21:24:12`     /series/DEXBZUS/comparisons/week/dollar_strength/current                                                      5.6449  2026-11-13
- `21:24:12`     /series/DEXBZUS/comparisons/year/dollar_strength/baseline                                                     5.2858  2025-11-13
- `21:24:12`     /series/DEXBZUS/comparisons/year/dollar_strength/current                                                      5.6449  2026-11-13
- `21:24:12`     /series/DEXCAUS/comparisons/month/dollar_strength/baseline                                                    1.4214  2026-10-13
- `21:24:12`     /series/DEXCAUS/comparisons/month/dollar_strength/current                                                     1.4256  2026-11-13
- `21:24:12`     /series/DEXCAUS/comparisons/quarter/dollar_strength/baseline                                                  1.3934  2026-08-13
- `21:24:12`     /series/DEXCAUS/comparisons/quarter/dollar_strength/current                                                   1.4256  2026-11-13
- `21:24:12`     /series/DEXCAUS/comparisons/week/dollar_strength/baseline                                                     1.4106  2026-11-06
- `21:24:12`     /series/DEXCAUS/comparisons/week/dollar_strength/current                                                      1.4256  2026-11-13
- `21:24:12`     /series/DEXCAUS/comparisons/year/dollar_strength/baseline                                                     1.401  2025-11-13
- `21:24:12`     /series/DEXCAUS/comparisons/year/dollar_strength/current                                                      1.4256  2026-11-13
- `21:24:12`     /series/DEXCHUS/comparisons/month/dollar_strength/baseline                                                    6.6937  2026-10-13
- `21:24:12`     /series/DEXCHUS/comparisons/month/dollar_strength/current                                                     6.671  2026-11-13
- `21:24:12`     /series/DEXCHUS/comparisons/quarter/dollar_strength/baseline                                                  6.7428  2026-08-13
- `21:24:12`     /series/DEXCHUS/comparisons/quarter/dollar_strength/current                                                   6.671  2026-11-13
- `21:24:12`     /series/DEXCHUS/comparisons/week/dollar_strength/baseline                                                     6.676  2026-11-06
- `21:24:12`     /series/DEXCHUS/comparisons/week/dollar_strength/current                                                      6.671  2026-11-13
- `21:24:12` data/crisis-plumbing.json | 364804 bytes, written 2026-11-22 15:42 | 77 measurements, 28 naming a dollar-shortage word | top level: {31: call, calls_eligible, comparisons, composite, context, contract, crisis_indices, decision, dependency_graph, engine}
- `21:24:12`     /comparisons/sofr_iorb                                         SOFR minus IORB                                1.0 basis_poin 
- `21:24:12`     /comparisons/usd_eur_overnight                                 USD SOFR minus euro €STR                       146.9 basis_poin 
- `21:24:12`     /measurements/ANFCI                                            Chicago Fed Adjusted National Financial Condit -0.502 Index 
- `21:24:12`     /measurements/BAMLC0A0CM                                       ICE BofA US Corporate Index Option-Adjusted Sp 0.94 Percent 
- `21:24:12`     /measurements/BAMLC0A4CBBB                                     ICE BofA BBB US Corporate Index Option-Adjuste 1.17 Percent 
- `21:24:12`     /measurements/BAMLEM4BRRBLCRPIOAS                              ICE BofA B & Lower Emerging Markets Corporate  6.49 Percent 
- `21:24:12`     /measurements/BAMLEMCBPIOAS                                    ICE BofA Emerging Markets Corporate Plus Index 1.84 Percent 
- `21:24:12`     /measurements/BAMLH0A0HYM2                                     ICE BofA US High Yield Index Option-Adjusted S 3.52 Percent 
- `21:24:12`     /measurements/BAMLH0A1HYBB                                     ICE BofA BB US High Yield Index Option-Adjuste 2.1 Percent 
- `21:24:12`     /measurements/BAMLH0A2HYB                                      ICE BofA Single-B US High Yield Index Option-A 3.8 Percent 
- `21:24:12`     /measurements/BAMLH0A3HYC                                      ICE BofA CCC & Lower US High Yield Index Optio 13.61 Percent 
- `21:24:12`     /measurements/DEXBZUS                                          Brazilian Reals to U.S. Dollar Spot Exchange R 5.6449 Brazilian  
- `21:24:12`     /measurements/DEXCHUS                                          Chinese Yuan Renminbi to U.S. Dollar Spot Exch 6.671 Chinese Yu 
- `21:24:12`     /measurements/DEXJPUS                                          Japanese Yen to U.S. Dollar Spot Exchange Rate 159.93 Japanese Y 
- `21:24:12`     /measurements/DEXKOUS                                          South Korean Won to U.S. Dollar Spot Exchange  1336.58 South Kore 
- `21:24:12`     /measurements/DEXUSEU                                          U.S. Dollars to Euro Spot Exchange Rate        1.1034 U.S. Dolla 
- `21:24:12`     /measurements/DFII10                                           Market Yield on U.S. Treasury Securities at 10 2.88 Percent 
- `21:24:12`     /measurements/DGS10                                            Market Yield on U.S. Treasury Securities at 10 5.04 Percent 
- `21:24:12`     /measurements/DGS3MO                                           Market Yield on U.S. Treasury Securities at 3- 4.34 Percent 
- `21:24:12`     /measurements/DTB3                                             3-Month Treasury Bill Secondary Market Rate, D 4.16 Percent 
- `21:24:12`     /measurements/DTWEXAFEGS                                       Nominal Advanced Foreign Economies U.S. Dollar 116.236 Index Jan  
- `21:24:12`     /measurements/DTWEXBGS                                         Nominal Broad U.S. Dollar Index                122.6503 Index Jan  
- `21:24:12` data/fx-decomposition.json | 5455 bytes, written 2026-11-21 22:00 | 0 measurements, 0 naming a dollar-shortage word | top level: {11: engine, version, generated_at, usd_state, carry_environment, dxy_decomposition, carry_pair_analyses, dxy_composition, methodology, academic_basis}
## B2. The Federal Reserve's dollar series in the warehouse, one at a time

- `21:24:13`   DTWEXBGS               5250 obs 2006-01-02 .. 2026-11-13 last 122.6503 | warehouse:fred-scoped/Exchange_Rates
- `21:24:14`   DTWEXAFEGS             5231 obs 2006-01-02 .. 2026-11-13 last 116.236 | warehouse:fred-scoped/Exchange_Rates
- `21:24:15`   DTWEXEMEGS             5231 obs 2006-01-02 .. 2026-11-13 last 130.8877 | warehouse:fred-scoped/Exchange_Rates
- `21:24:16`   DEXUSEU                6988 obs 1999-01-04 .. 2026-11-13 last 1.1034 | warehouse:fred-scoped/Exchange_Rates
- `21:24:17`   DEXJPUS               14003 obs 1971-01-04 .. 2026-11-13 last 159.93 | warehouse:fred-scoped/Exchange_Rates
- `21:24:19`   DEXUSUK               14009 obs 1971-01-04 .. 2026-11-13 last 1.3212 | warehouse:fred-scoped/Exchange_Rates
- `21:24:20`   DEXCAUS               14015 obs 1971-01-04 .. 2026-11-13 last 1.4256 | warehouse:fred-scoped/Exchange_Rates
- `21:24:21`   DEXSDUS               14008 obs 1971-01-04 .. 2026-11-13 last 10.075 | warehouse:fred-scoped/Exchange_Rates
- `21:24:22`   DEXSZUS               14009 obs 1971-01-04 .. 2026-11-13 last 0.8389 | warehouse:fred-scoped/Exchange_Rates
- `21:24:46`   SWPT                   1249 obs 2002-12-18 .. 2026-11-18 last 146.0 | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, bank heale
- `21:24:48`   WLRRAFOIAL             1249 obs 2002-12-18 .. 2026-11-18 last 316712.0 | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, bank heale
- `21:24:49`   WSEFINTL1              1249 obs 2002-12-18 .. 2026-11-18 last 2813231.0 | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, bank heale
- `21:24:51`   WMTSECL1               1249 obs 2002-12-18 .. 2026-11-18 last 2544011.0 | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, bank heale
- `21:25:15`   WFASECL1               1249 obs 2002-12-18 .. 2026-11-18 last 196449.0 | warehouse:fred-scoped/Monetary_Data (+15 obs tail from FRED, bank heal
- `21:25:17`   H41RESPPALGTRFNWW      1249 obs 2002-12-18 .. 2026-11-18 last 1.0 | warehouse:fred-scoped/Monetary_Data (+1 obs tail from FRED, bank heale
- `21:25:19`   WLFOL                   809 obs 2002-12-18 .. 2018-06-13 last 5255.0 | fred-api (banked on demand → warehouse)
- `21:25:20`   WDFOL                  1249 obs 2002-12-18 .. 2026-11-18 last 9479.0 | warehouse:fred-scoped/Monetary_Data (+7 obs tail from FRED, bank heale
- `21:25:22`   SOFR                   2156 obs 2018-04-03 .. 2026-11-19 last 3.91 | warehouse:fred-scoped/Interest_Rates
- `21:25:23`   IORB                   1944 obs 2021-07-29 .. 2026-11-23 last 3.9 | warehouse:fred-scoped/Interest_Rates
- `21:25:25`   EFFR                   6624 obs 2000-07-03 .. 2026-11-19 last 3.88 | warehouse:fred-scoped/Interest_Rates
- `21:25:26`   OBFR                   2693 obs 2016-03-01 .. 2026-11-18 last 3.88 | warehouse:fred-scoped/Interest_Rates
- `21:25:28`   TEDRATE                8853 obs 1986-01-02 .. 2022-01-21 last 0.09 | warehouse:fred-scoped/On_Demand
- `21:25:29`   DGS3MO                11304 obs 1981-09-01 .. 2026-11-19 last 4.34 | warehouse:fred-scoped/Interest_Rates
- `21:25:31`   RRPONTSYD              3370 obs 2003-02-07 .. 2026-11-18 last 0.331 | warehouse:fred-scoped/Monetary_Data
- `21:25:31` resolved with history: 24 of 24
## B3. Titles in the warehouse's FRED catalog

- `21:25:32` 2229 objects under data/warm/fred-catalog/, 429.2 MB: ['series-meta/page-0000.json', 'series-meta/page-0001.json', 'series-meta/page-0002.json', 'series-meta/page-0003.json', 'series-meta/page-0004.json', 'series-meta/page-0005.json', 'series-meta/page-0006.json', 'series-meta/page-0007.json']
- `21:29:18` scanned 2229 catalog files; 459 title hits
- `21:29:18`   RACBLS                 Resources and Assets: Central Bank Liquidity Swaps |  | ..
- `21:29:18`   H41RESPPALGTRFNWW      Assets: Other: Repurchase Agreements - Foreign Official: Wednesday Level |  | ..
- `21:29:18`   H41RESPPALGTRFXAWNWW   Assets: Other: Repurchase Agreements - Foreign Official: Week Average |  | ..
- `21:29:18`   H41RESPPALGTRFXAWXCH1NWW Assets: Other: Repurchase Agreements - Foreign Official: Change in Week Average from Previous Week Average |  | ..
- `21:29:18`   H41RESPPALGTRFXAWXCH52NWW Assets: Other: Repurchase Agreements - Foreign Official: Change in Week Average from Year Ago Week Average |  | ..
- `21:29:18`   RESH4SCSXAWXCH1NWW     Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Change in Week Average from Previous Week  |  | ..
- `21:29:18`   RESH4SCSXAWXCH52NWW    Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Change in Week Average from Year Ago Week  |  | ..
- `21:29:18`   RESH4SCSXCH1NWW        Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Change in Wednesday Level from Previous We |  | ..
- `21:29:18`   RESH4SCSXCH52NWW       Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Change in Wednesday Level from Year Ago Le |  | ..
- `21:29:18`   RESPPLLDFXAWXCH1NWW    Liabilities and Capital: Liabilities: Deposits with F.R. Banks, Other Than Reserve Balances: Foreign Official: |  | ..
- `21:29:18`   RESPPLLDFXAWXCH52NWW   Liabilities and Capital: Liabilities: Deposits with F.R. Banks, Other Than Reserve Balances: Foreign Official: |  | ..
- `21:29:18`   RESPPLLDFXCH1NWW       Liabilities and Capital: Liabilities: Deposits with F.R. Banks, Other Than Reserve Balances: Foreign Official: |  | ..
- `21:29:18`   RESPPLLDFXCH52NWW      Liabilities and Capital: Liabilities: Deposits with F.R. Banks, Other Than Reserve Balances: Foreign Official: |  | ..
- `21:29:18`   RESPPLLRFXAWXCH1NWW    Liabilities and Capital: Liabilities: Reverse Repurchase Agreements: Foreign Official and International Accoun |  | ..
- `21:29:18`   RESPPLLRFXAWXCH52NWW   Liabilities and Capital: Liabilities: Reverse Repurchase Agreements: Foreign Official and International Accoun |  | ..
- `21:29:18`   WACBS                  Assets: Central Bank Liquidity Swaps (DISCONTINUED) |  | ..
- `21:29:18`   WCBLSA                 Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Week Average |  | ..
- `21:29:18`   WDFOA                  Liabilities and Capital: Liabilities: Deposits with F.R. Banks, Other Than Reserve Balances: Foreign Official: |  | ..
- `21:29:18`   WDFOL                  Liabilities and Capital: Liabilities: Deposits with F.R. Banks, Other Than Reserve Balances: Foreign Official: |  | ..
- `21:29:18`   WLIQSWP                Reserve Bank Credit: Central Bank Liquidity Swaps (DISCONTINUED) |  | ..
- `21:29:18`   WLRRAFOIAL             Liabilities and Capital: Liabilities: Reverse Repurchase Agreements: Foreign Official and International Accoun |  | ..
- `21:29:18`   WREPOFOR               Liabilities and Capital: Liabilities: Reverse Repurchase Agreements: Foreign Official and International Accoun |  | ..
- `21:29:18`   WSEFINT1               Memorandum Items: Custody Holdings: Securities in Custody for Foreign and International Accounts: Week Average |  | ..
- `21:29:18`   WSEFINTL1              Memorandum Items: Custody Holdings: Securities in Custody for Foreign and International Accounts: Wednesday Le |  | ..
- `21:29:18`   H0RESH4CNWW            Memorandum Items: Custody Holdings: Total (Marketable Securities Held in Custody for Foreign Official and Inte |  | ..
- `21:29:18`   H0RESH4CXAWNWW         Memorandum Items: Custody Holdings: Total (Marketable Securities Held in Custody for Foreign Official and Inte |  | ..
- `21:29:18`   H0RESH4CXAWXCH1NWW     Memorandum Items: Custody Holdings: Total (Marketable Securities Held in Custody for Foreign Official and Inte |  | ..
- `21:29:18`   H0RESH4CXAWXCH52NWW    Memorandum Items: Custody Holdings: Total (Marketable Securities Held in Custody for Foreign Official and Inte |  | ..
- `21:29:18`   RESH4FXAWXCH1NWW       Memorandum Items: Custody Holdings: Securities in Custody for Foreign and International Accounts: Change in We |  | ..
- `21:29:18`   RESH4FXAWXCH52NWW      Memorandum Items: Custody Holdings: Securities in Custody for Foreign and International Accounts: Change in We |  | ..
- `21:29:18`   SWP10Y                 Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Maturing in over 10 Years: Wednesday Level |  | ..
- `21:29:18`   SWP15                  Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Maturing Within 15 Days: Wednesday Level |  | ..
- `21:29:18`   SWP1690                Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Maturing in 16 Days to 90 Days: Wednesday  |  | ..
- `21:29:18`   SWP1T5                 Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Maturing in over 1 Year to 5 Years: Wednes |  | ..
- `21:29:18`   SWP5T10                Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Maturing in over 5 Years to 10 Years: Wedn |  | ..
- `21:29:18`   SWP911Y                Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Maturing in 91 Days to 1 Year: Wednesday L |  | ..
- `21:29:18`   SWPT                   Assets: Central Bank Liquidity Swaps: Central Bank Liquidity Swaps: Wednesday Level |  | ..
- `21:29:18`   BOGZ1FA713090003A      Central Bank; Other Assets, Including Fixed Assets, Foreign Currency Denominated Assets and Central Bank Liqui |  | ..
- `21:29:18`   BOGZ1FA713090003Q      Central Bank; Other Assets, Including Fixed Assets, Foreign Currency Denominated Assets and Central Bank Liqui |  | ..
- `21:29:18`   BOGZ1FL713090003A      Central Bank; Other Assets, Including Fixed Assets, Foreign Currency Denominated Assets and Central Bank Liqui |  | ..
- `21:29:18`   BOGZ1FL713090003Q      Central Bank; Other Assets, Including Fixed Assets, Foreign Currency Denominated Assets and Central Bank Liqui |  | ..
- `21:29:18`   BOGZ1FR713090003A      Central Bank; Other Assets, Including Fixed Assets, Foreign Currency Denominated Assets and Central Bank Liqui |  | ..
- `21:29:18`   BOGZ1FR713090003Q      Central Bank; Other Assets, Including Fixed Assets, Foreign Currency Denominated Assets and Central Bank Liqui |  | ..
- `21:29:18`   BOGZ1FU713090003A      Central Bank; Other Assets, Including Fixed Assets, Foreign Currency Denominated Assets and Central Bank Liqui |  | ..
- `21:29:18`   BOGZ1FU713090003Q      Central Bank; Other Assets, Including Fixed Assets, Foreign Currency Denominated Assets and Central Bank Liqui |  | ..
- `21:29:18` ✅ verification and audit complete; nothing written
