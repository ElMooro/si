# ops 6946 - Credit Before Equity 2.1.0: the stress rank and the AI stack go live, 2026-11-24T23:50+00:00

**Status:** success  
**Duration:** 38.9s  
**Finished:** 2026-11-24T23:51:26+00:00  

## Data

| ai_stack | cds_levels | companies | document | failed_checks | leads | ledger_days | listed | under_stress |
|---|---|---|---|---|---|---|---|---|
| 66 | 203 | 2946 | 2026-11-24T23:51 | 0 | 47 | 15 | 1085 | 304 |

## Log
## 1. Guard: the deployed function is this checkout's

- `23:50:49` justhodl-credit-before-equity updated 2026-11-24T23:48:17 | memory 2048 MB, timeout 300 s | files as in this checkout: {'lambda_function.py': True, 'cbe_core.py': True, 'cbe_names.py': True, 'cbe_themes.py': True, 'leaders_tape.py': True} | shared modules in the package: ['bank_enrich.py']
- `23:50:49` schedule: justhodl-credit-before-equity-daily cron(35 22 * * ? *) ENABLED
- `23:50:50` before: data/credit-before-equity.json version 2.0.0, 1332171 bytes, generated 2026-11-24T22:35:41.941837+00:00, 1090 names, 48 leads | ledger days 15 (2025-12-31 .. 2026-11-23)
## 2. One dry run inside Lambda (nothing written)

- `23:51:07` HTTP 200, function error None, 17.6 s | {"statusCode": 200, "dry_run": true, "as_of": "2026-11-23", "companies": 2946, "listed": 1085, "names": 1090, "leads": 47, "cds_levels": 203, "windows": {"w1": "2026-11-16", "m1": "2026-10-30", "m3": null, "ytd": "2025-12-31"}, "degraded": [], "ledger_days": ["2026-11-23"], "bytes": 1547950, "version": "2.1.0", "stressed": 304, "themes": {"ai": 66}}
- `23:51:07`   lambda: [cbe] stocks: 1090 of 1090 read from the price bank, newest close 2026-11-23, 1 splits rebased, 0 with no file
- `23:51:07`   lambda: REPORT RequestId: d097c25c-4520-4e98-b4c9-c465269b4f4d	Duration: 16914.08 ms	Billed Duration: 17289 ms	Memory Size: 2048 MB	Max Memory Used: 267 MB	Init Duration: 374.03 ms
- `23:51:08` the document, the ledger and the model ledger are what they were: the dry run wrote nothing
## 3. One publication (a one-off run outside the 22:35 UTC schedule)

- `23:51:24` HTTP 200, function error None, 16.2 s | {"statusCode": 200, "body": "{\"names\": 1090, \"leads\": 47, \"companies\": 2946, \"as_of\": \"2026-11-23\"}"}
- `23:51:24`   lambda: [cbe] stocks: 1090 of 1090 read from the price bank, newest close 2026-11-23, 1 splits rebased, 0 with no file
- `23:51:24`   lambda: [cbe] 2026-11-23: 2946 companies (1085 listed), 47 leads, 203 CDS levels, 1547950 bytes; ledger days written ['2026-11-23']
- `23:51:24`   lambda: REPORT RequestId: 00e9915b-33b7-4114-a83a-11382ce1eaa3	Duration: 16129.71 ms	Billed Duration: 16130 ms	Memory Size: 2048 MB	Max Memory Used: 284 MB
## 4. Read back

- `23:51:25` data/credit-before-equity.json: 1547950 bytes (305819 gzipped) written 23:51:25 | credit-before-equity.v2 2.1.0 | generated 2026-11-24T23:51:08.673414+00:00 | bonds of 2026-11-23, stocks to 2026-11-23
- `23:51:25` 2946 companies (1085 listed, 1861 credit only), 14283 bonds with a spread, 47 leads, 203 CDS levels (141 tied) | degraded []
- `23:51:25` windows: {'w1': ('2026-11-16', "the board's own ledger"), 'm1': ('2026-10-30', "worked back from the bond desk's own yield change since that day"), 'm3': (None, "The board's ledger holds no day 91 days back yet, give or take 21; one will be from about 2027-01-08."), 'ytd': ('2025-12-31', "worked back from the bond desk's own yield change since that day")}
- `23:51:25` ledger days now 15: ['2025-12-31', '2026-10-30', '2026-11-05', '2026-11-06', '2026-11-09', '2026-11-10', '2026-11-11', '2026-11-12', '2026-11-13', '2026-11-16', '2026-11-17', '2026-11-18', '2026-11-19', '2026-11-20', '2026-11-23']
- `23:51:25` stress: {"long": "ytd", "long_ref": "2025-12-31", "stressed_at": 80.0, "book_spread_bp": {"em": 258.4, "hy": 253.0, "ig": 98.2}, "n_stressed": 304, "n_stressed_listed": 67} | 304 at 80.0 or more (67 listed)
- `23:51:25` AI stack: 66 companies (48 listed), $9.3B held, middle spread 151.9 bp, 9 under stress holding 24.0% of its value: [['Oracle Corporation', 99.6], ['SOFTBANK GROUP CORP', 83.0], ['Intel Corp.', 86.8]] | year {'d_bp': 37.3, 'from_bp': 83.2, 'd_pct': 44.8, 'n': 48, 'wider': 36, 'tighter': 7, 'widest': ['CoreWeave, Inc. Class A Common Stock', 321.2], 'tightest': ['APLD COMPUTECO LLC', -306.6]}
- `23:51:25`   ORCL   Oracle Corporation                   ig $  851M spread   327.6 bp, year 149.1 bp (84.4%), month 51.6 bp, CDS 329.8 bp | stress 99.6 | stock -13.0% in the month | ai -> NONE
- `23:51:25`   IEP    Icahn Enterprises L.P.               hy $   99M spread  2064.0 bp, year 1428.6 bp (224.8%), month 1207.1 bp, CDS None bp | stress 98.4 | stock -42.58% in the month |  -> NONE
- `23:51:25`          CONNECT HOLDING II LLC               hy $   53M spread  2277.6 bp, year 1466.3 bp (180.7%), month 342.9 bp, CDS None bp | stress 97.4 | stock None% in the month |  -> None
- `23:51:25`   FMC    FMC Corporation                      hy $   73M spread  1152.3 bp, year 797.2 bp (224.3%), month 472.7 bp, CDS None bp | stress 96.5 | stock -33.55% in the month |  -> NONE
- `23:51:25`   CYH    Community Health Systems, Inc.       hy $  245M spread  1274.4 bp, year 811.4 bp (175.3%), month 356.8 bp, CDS 2380.2 bp | stress 96.1 | stock -31.25% in the month |  -> NONE
- `23:51:25`   GT     The Goodyear Tire & Rubber Company   hy $  134M spread   875.7 bp, year 616.2 bp (283.5%), month 259.5 bp, CDS 222.7 bp | stress 95.8 | stock -14.34% in the month |  -> NONE
- `23:51:25`   SPCX   Space Exploration Technologies Corp. ig $  226M spread   275.7 bp, year None bp (None%), month 15.3 bp, CDS None bp | stress 95.5 | stock -1.02% in the month |  -> NONE
- `23:51:25`   NAVI   Navient Corporation                  hy $  142M spread   836.8 bp, year 560.4 bp (235.6%), month 304.8 bp, CDS None bp | stress 95.1 | stock -30.86% in the month |  -> NONE
- `23:51:25`   UNIT   Uniti Group Inc.                     hy $  226M spread   877.2 bp, year 513.5 bp (141.2%), month 258.1 bp, CDS None bp | stress 94.3 | stock -31.03% in the month |  -> NONE
- `23:51:25`   OPTU   Optimum Communications, Inc.         hy $  181M spread  2622.8 bp, year 1246.3 bp (90.5%), month 87.6 bp, CDS None bp | stress 93.2 | stock -23.99% in the month |  -> NONE
- `23:51:25`          GLOBAL AUTO HOLDINGS PLC             hy $   56M spread  1026.5 bp, year 452.0 bp (78.7%), month 335.3 bp, CDS None bp | stress 93.2 | stock None% in the month |  -> None
- `23:51:25`          GOEASY LTD                           hy $   84M spread   850.5 bp, year 410.1 bp (93.1%), month 277.0 bp, CDS None bp | stress 92.8 | stock None% in the month |  -> None
- `23:51:25` leads held back because a liquid CDS says the opposite of the bonds: [('AMD', -16.9, 38.3)]
- `23:51:25` check the document is 2.1 on the v2 contract                         ok
- `23:51:25` check the 2.0 columns are where they were, the 2.1 columns after them ok
- `23:51:25` check the stress counts are the rows                                 ok
- `23:51:25` check every rank is between 0 and 100                                ok
- `23:51:25` check the AI stack's count is its rows and its layers                ok
- `23:51:25` check Oracle is in the theme and under stress, and is not a lead     ok
- `23:51:25` check no lead where a liquid CDS says the opposite                   ok
- `23:51:25` check the industries carry the stress figures                        ok
- `23:51:25` check a window that is not there yet says when it can be             ok
- `23:51:25` check the counts are the rows                                        ok
- `23:51:25` check it is the size the runner's dry run saw                        ok
- `23:51:25` check every key the first engine published is there                  ok
- `23:51:25` check the model's names kept their fields                            ok
- `23:51:25` check the run was not degraded                                       ok
- `23:51:25` check the ledger holds today's holdings day and the two reference days ok
- `23:51:25` check a one-week comparison exists from the board's own ledger       ok
- `23:51:25` check no ledger day was removed                                      ok
- `23:51:26` the site serves the document generated 2026-11-24T23:51:08.673414+00:00 -> the same
- `23:51:26`   lead PZZA   Papa John's International, Inc.  spread 552.1 bp, +158.0 bp in the month (+147.9 past its book), stock +4.42% -> CREDIT_LEADS_DOWN
- `23:51:26`   lead SGRY   Surgery Partners, Inc.           spread 242.6 bp, +113.7 bp in the month (+103.6 past its book), stock +14.43% -> CREDIT_LEADS_DOWN
- `23:51:26`   lead BHC    Bausch Health Companies Inc.     spread 592.0 bp, +74.2 bp in the month (+64.1 past its book), stock +1.59% -> CREDIT_LEADS_DOWN
- `23:51:26`   lead MLCO   Melco Resorts & Entertainment Li spread 344.1 bp, +71.7 bp in the month (+61.6 past its book), stock +2.54% -> CREDIT_LEADS_DOWN
- `23:51:26`   lead GEO    The GEO Group, Inc.              spread 274.0 bp, +61.8 bp in the month (+51.7 past its book), stock +4.28% -> CREDIT_LEADS_DOWN
- `23:51:26`   lead MATV   Mativ Holdings, Inc.             spread 309.9 bp, +59.2 bp in the month (+49.1 past its book), stock -2.29% -> CREDIT_LEADS_DOWN
- `23:51:26`   lead PRGO   Perrigo Company plc              spread 239.7 bp, +54.8 bp in the month (+44.7 past its book), stock +0.38% -> CREDIT_LEADS_DOWN
- `23:51:26`   lead CODI   Compass Diversified              spread 335.8 bp, +45.7 bp in the month (+35.6 past its book), stock +20.72% -> CREDIT_LEADS_DOWN
- `23:51:26` ✅ 2.1.0 LIVE: 2946 companies (1085 listed), 304 under stress (67 listed), AI stack 66 companies with 9 under stress, 47 leads; one dry run and one publication, both by hand, disclosed
