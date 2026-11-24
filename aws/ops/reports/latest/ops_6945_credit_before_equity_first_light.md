# ops 6945 - Credit Before Equity 2.0.0: first light of the deployed engine, 2026-11-24T21:15+00:00

**Status:** success  
**Duration:** 44.5s  
**Finished:** 2026-11-24T21:16:27+00:00  

## Data

| cds_levels | companies | document | failed_checks | leads | ledger_days | listed |
|---|---|---|---|---|---|---|
| 203 | 2946 | 2026-11-24T21:16 | 0 | 48 | 15 | 1085 |

## Log
## 1. Guard: the deployed function is this checkout's

- `21:15:43` justhodl-credit-before-equity updated 2026-11-24T21:14:46 | memory 2048 MB, timeout 300 s | files as in this checkout: {'lambda_function.py': True, 'cbe_core.py': True, 'cbe_names.py': True, 'leaders_tape.py': True} | shared modules in the package: ['bank_enrich.py']
- `21:15:43` schedule: justhodl-credit-before-equity-daily cron(35 22 * * ? *) ENABLED
- `21:15:44` before: data/credit-before-equity.json version 1.0.0, 10551 bytes, generated 2026-11-23T22:35:42.018058+00:00, 26 names, 0 leads | ledger days 13 (2026-11-05 .. 2026-11-23)
## 2. One dry run inside Lambda (nothing written)

- `21:16:05` HTTP 200, function error None, 21.3 s | {"statusCode": 200, "dry_run": true, "as_of": "2026-11-23", "companies": 2946, "listed": 1085, "names": 1090, "leads": 48, "cds_levels": 203, "windows": {"w1": "2026-11-16", "m1": "2026-10-30", "m3": null, "ytd": "2025-12-31"}, "degraded": [], "ledger_days": ["2026-11-23", "2025-12-31", "2026-10-30"], "bytes": 1332171}
- `21:16:05`   lambda: [cbe] stocks: 1090 of 1090 read from the price bank, newest close 2026-11-23, 1 splits rebased, 0 with no file
- `21:16:05`   lambda: REPORT RequestId: 2305d052-8075-4e86-b0dc-a05bf87e85e8	Duration: 20768.76 ms	Billed Duration: 21106 ms	Memory Size: 2048 MB	Max Memory Used: 261 MB	Init Duration: 336.33 ms
- `21:16:05` the document, the ledger and the model ledger are what they were: the dry run wrote nothing
## 3. One publication (a one-off run outside the 22:35 UTC schedule)

- `21:16:26` HTTP 200, function error None, 20.3 s | {"statusCode": 200, "body": "{\"names\": 1090, \"leads\": 48, \"companies\": 2946, \"as_of\": \"2026-11-23\"}"}
- `21:16:26`   lambda: [cbe] stocks: 1090 of 1090 read from the price bank, newest close 2026-11-23, 1 splits rebased, 0 with no file
- `21:16:26`   lambda: [cbe] 2026-11-23: 2946 companies (1085 listed), 48 leads, 203 CDS levels, 1332171 bytes; ledger days written ['2026-11-23', '2025-12-31', '2026-10-30']
- `21:16:26`   lambda: REPORT RequestId: edc569ad-ea0c-47a3-9093-d3145b91cd91	Duration: 20265.70 ms	Billed Duration: 20266 ms	Memory Size: 2048 MB	Max Memory Used: 277 MB
## 4. Read back

- `21:16:26` data/credit-before-equity.json: 1332171 bytes (246602 gzipped) written 21:16:27 | credit-before-equity.v2 2.0.0 | generated 2026-11-24T21:16:05.988786+00:00 | bonds of 2026-11-23, stocks to 2026-11-23
- `21:16:26` 2946 companies (1085 listed, 1861 credit only), 14283 bonds with a spread, 48 leads, 203 CDS levels (141 tied) | degraded []
- `21:16:26` windows: {'w1': ('2026-11-16', "the board's own ledger"), 'm1': ('2026-10-30', "worked back from the bond desk's own yield change since that day"), 'm3': (None, "The board's ledger holds no day near enough yet; it began on 2026-11-05 and adds a day each run."), 'ytd': ('2025-12-31', "worked back from the bond desk's own yield change since that day")}
- `21:16:26` ledger days now 15: ['2025-12-31', '2026-10-30', '2026-11-05', '2026-11-06', '2026-11-09', '2026-11-10', '2026-11-11', '2026-11-12', '2026-11-13', '2026-11-16', '2026-11-17', '2026-11-18', '2026-11-19', '2026-11-20', '2026-11-23']
- `21:16:26` check the document is the 2.0 contract                               ok
- `21:16:26` check the counts are the rows                                        ok
- `21:16:26` check it is the size the runner's dry run saw                        ok
- `21:16:26` check every key the first engine published is there                  ok
- `21:16:26` check the model's names kept their fields                            ok
- `21:16:26` check the run was not degraded                                       ok
- `21:16:26` check the ledger holds today's holdings day and the two reference days ok
- `21:16:26` check a one-week comparison exists from the board's own ledger       ok
- `21:16:26` check no ledger day was removed                                      ok
- `21:16:26` check the model ledger was appended                                  ok
- `21:16:27` the site serves the document generated 2026-11-24T21:16:05.988786+00:00 -> the same
- `21:16:27`   lead PZZA   Papa John's International, Inc.  spread 552.1 bp, +158.0 bp in the month (+147.9 past its book), stock +4.42% -> CREDIT_LEADS_DOWN
- `21:16:27`   lead SGRY   Surgery Partners, Inc.           spread 242.6 bp, +113.7 bp in the month (+103.6 past its book), stock +14.43% -> CREDIT_LEADS_DOWN
- `21:16:27`   lead BHC    Bausch Health Companies Inc.     spread 592.0 bp, +74.2 bp in the month (+64.1 past its book), stock +1.59% -> CREDIT_LEADS_DOWN
- `21:16:27`   lead MLCO   Melco Resorts & Entertainment Li spread 344.1 bp, +71.7 bp in the month (+61.6 past its book), stock +2.54% -> CREDIT_LEADS_DOWN
- `21:16:27`   lead GEO    The GEO Group, Inc.              spread 274.0 bp, +61.8 bp in the month (+51.7 past its book), stock +4.28% -> CREDIT_LEADS_DOWN
- `21:16:27`   lead MATV   Mativ Holdings, Inc.             spread 309.9 bp, +59.2 bp in the month (+49.1 past its book), stock -2.29% -> CREDIT_LEADS_DOWN
- `21:16:27`   lead PRGO   Perrigo Company plc              spread 239.7 bp, +54.8 bp in the month (+44.7 past its book), stock +0.38% -> CREDIT_LEADS_DOWN
- `21:16:27`   lead CODI   Compass Diversified              spread 335.8 bp, +45.7 bp in the month (+35.6 past its book), stock +20.72% -> CREDIT_LEADS_DOWN
- `21:16:27` ✅ FIRST LIGHT: 2.0.0 published 2946 companies (1085 listed) with 48 leads and 203 CDS levels; one dry run and one publication, both by hand, disclosed
