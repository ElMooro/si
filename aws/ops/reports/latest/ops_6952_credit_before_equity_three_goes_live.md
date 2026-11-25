# ops 6952 - Credit Before Equity 3.0.0: the record, the chance of a fall, more companies and every figure go live, 2026-11-25T17:15+00:00

**Status:** success  
**Duration:** 198.0s  
**Finished:** 2026-11-25T17:18:33+00:00  

## Data

| cds_levels | chances | companies | document | failed_checks | leads | listed | month_ends | non_dollar | run_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 211 | 1083 | 4184 | 2026-11-25T17:15 | 0 | 56 | 1107 | 63 | 1239 | 168 |

## Log
## 1. Guard: the deployed function is this checkout's

- `17:15:16` justhodl-credit-before-equity updated 2026-11-25T17:10:26 | memory 3008 MB, timeout 900 s | files as in this checkout: {'lambda_function.py': True, 'cbe_core.py': True, 'cbe_names.py': True, 'cbe_themes.py': True, 'cbe_study.py': True, 'leaders_tape.py': True} | shared modules in the package: ['bank_enrich.py']
- `17:15:16` schedule: justhodl-credit-before-equity-daily cron(35 22 * * ? *) ENABLED
- `17:15:17` before: data/credit-before-equity.json version 2.1.0, 1547950 bytes, generated 2026-11-24T23:51:08.673414+00:00, holdings of 2026-11-23, 2946 companies, 1090 names, 47 leads | ledger days 76 (2021-07-30 .. 2026-11-23) | dated holdings 64 | days of calls 0 | evidence there: False
## 2. One dry run inside Lambda (nothing written)

- `17:15:38` HTTP 200, function error None, 21.2 s | {"statusCode": 200, "dry_run": true, "as_of": "2026-11-23", "companies": 4184, "listed": 1107, "names": 1112, "leads": 56, "cds_levels": 211, "windows": {"w1": "2026-11-16", "m1": "2026-10-30", "m3": "2026-08-31", "ytd": "2025-12-31"}, "degraded": [], "ledger_days": ["2026-11-23"], "bytes": 2736260, "version": "3.0.0", "stressed": 391, "themes": {"ai": 70}, "evidence": false, "study_due": "the evidence has never been written", "forecasts": null, "bonds_rows": 21106, "calls": 0}
- `17:15:38`   lambda: [cbe] stocks: 1112 of 1112 read from the price bank, newest close 2026-11-24, 1 splits rebased, 0 with no file
- `17:15:38`   lambda: REPORT RequestId: 3f8d820e-02ba-4eee-8978-bd1e1ee5a537	Duration: 20433.09 ms	Billed Duration: 20840 ms	Memory Size: 3008 MB	Max Memory Used: 294 MB	Init Duration: 406.11 ms
- `17:15:39` the document, the ledger, the dated holdings, the calls and the model ledger are what they were: the dry run wrote nothing
## 3. One publication (a one-off run outside the 22:35 UTC schedule; the study runs inside it when the evidence is due)

- `17:18:27` HTTP 200, function error None, 167.8 s | {"statusCode": 200, "body": "{\"names\": 1112, \"leads\": 56, \"companies\": 4184, \"as_of\": \"2026-11-23\", \"study\": true}"}
- `17:18:27`   lambda: [cbe] stocks: 1112 of 1112 read from the price bank, newest close 2026-11-24, 1 splits rebased, 0 with no file
- `17:18:27`   lambda: [cbe] 2026-11-23: 4184 companies (1107 listed), 56 leads, 211 CDS levels, 1083 forecasts, 2790694 bytes; ledger days written ['2026-11-23']; study redone
- `17:18:27`   lambda: REPORT RequestId: 85ece166-ee01-4a1c-b10b-2afc32a31730	Duration: 167727.63 ms	Billed Duration: 167728 ms	Memory Size: 3008 MB	Max Memory Used: 939 MB
## 4. Read back

- `17:18:28` data/credit-before-equity.json: 2790694 bytes (601224 gzipped) written 17:18:27 | credit-before-equity.v2 3.0.0 | generated 2026-11-25T17:15:39.766329+00:00 | bonds of 2026-11-23, stocks to 2026-11-24
- `17:18:28` 4184 companies (1107 listed, 3077 credit only), 19622 bonds with a spread (5339 not in dollars), 56 leads, 211 CDS levels (148 tied) | by book {'ig': 1683, 'hy': 922, 'em': 340, 'xig': 942, 'xhy': 297} | by currency {'AUD': 41, 'CAD': 130, 'EUR': 911, 'GBP': 157, 'USD': 2945} | degraded []
- `17:18:28` windows: {'w1': ('2026-11-16', "the board's own ledger"), 'm1': ('2026-10-30', "the board's own ledger; 5212 bonds it did not hold that day are worked back from the bond desk's own yield change"), 'm3': ('2026-08-31', "the board's own ledger"), 'ytd': ('2025-12-31', "the board's own ledger; 3383 bonds it did not hold that day are worked back from the bond desk's own yield change")}
- `17:18:28` curves: {"as_of": "2026-11-23", "source": "home.treasury.gov daily_treasury_yield_curve", "other": {"AUD": {"as_of": "2026-11-18", "words": "Australian government yields at 2, 3, 5 and 10 years"}, "CAD": {"as_of": "2026-11-23", "words": "Government of Canada benchmark yields at 2, 5, 10 and 30 years"}, "EUR": {"as_of": "2026-11-23", "words": "German Bund yields at 2, 10 and 30 years"}, "GBP": {"as_of": "2026-11-20", "words": "UK gilt par yields at 5, 10 and 20 years"}}}
- `17:18:29` against the document it replaces (holdings of 2026-11-23 then, 2026-11-23 now): 2946 companies were on the board, 0 are no longer, 0 changed book, industry or tie, 0 have another spread; 1238 companies are new
- `17:18:30` the record: 63 month-ends 2021-07-30 .. 2026-10-30, 62444 company-months of 1149 companies; measured in 146.2 s inside Lambda (the whole run took 168 s) | left out [['2022-06-30', '2534 of 11757 bonds with a spread']]
- `17:18:30`   Of the 11 readings of what credit and the stock just did, tried over one month and over three, none tells the stock's direction with a margin past chance (3 standard errors, the bar for 22 tries at once).
- `17:18:30`   a lead down: -0.06% over the next month against the board's average stock, 1836 company-months, how sure 0.5 | a lead up: 0.42%, how sure 0.45
- `17:18:30`   fell 20% inside three months, by spread band: under 100 bp 9.3% of 17038; 100 to 200 bp 14.2% of 21839; 200 to 400 bp 22.7% of 13782; 400 to 700 bp 33.7% of 3203; 700 to 1,000 bp 41.2% of 628; 1,000 bp or more 52.1% of 468 | the whole board 16.5%
- `17:18:30`   the check: said 5.3% happened 4.7% (10561); said 12.7% happened 12.4% (11441); said 27.0% happened 26.7% (9686); said 39.8% happened 37.3% (1587); the faller had the higher chance 69.5% of the time; 33275 company-months over 33 months
- `17:18:30`   traded CDS: 236 names, 111 weeks, 12031 name-weeks; CDS wider over the week: next week 0.03% (how sure 0.36); CDS wider, stock rose: next week -0.14% (how sure -0.36); CDS wider, stock flat: next week 0.13% (how sure 0.91); CDS wider, stock already fell: next week -0.01% (how sure 0.48); CDS tighter over the week: next week -0.04% (how sure -0.13)
- `17:18:30` the chance of a fall: 1083 listed companies have one (1083 declared), 0 differ from the record's table; 63 at one in three or more; volatility thirds cut at [29.2, 42.9]
- `17:18:30`   CRWV   CoreWeave, Inc. Class A Common Stock hy spread  1082.8 bp, own volatility 109.9 ->  52.6% in 3 months,  29.0% in one | stress 85.5 | flat/down | stock -22.35% in the month
- `17:18:30`   CYH    Community Health Systems, Inc.       hy spread  1274.4 bp, own volatility  67.8 ->  52.6% in 3 months,  29.0% in one | stress 96.1 | worse/down | stock -26.25% in the month
- `17:18:30`   OPTU   Optimum Communications, Inc.         hy spread  2622.8 bp, own volatility 136.6 ->  52.6% in 3 months,  29.0% in one | stress 93.1 | flat/down | stock -14.04% in the month
- `17:18:30`   IEP    Icahn Enterprises L.P.               hy spread  2064.0 bp, own volatility  74.9 ->  52.6% in 3 months,  29.0% in one | stress 98.5 | worse/down | stock -47.4% in the month
- `17:18:30`   FMC    FMC Corporation                      hy spread  1152.3 bp, own volatility 114.7 ->  52.6% in 3 months,  29.0% in one | stress 96.6 | worse/down | stock -32.74% in the month
- `17:18:30`   UNIT   Uniti Group Inc.                     hy spread   877.2 bp, own volatility  77.6 ->  42.2% in 3 months,  18.4% in one | stress 94.2 | worse/down | stock -21.54% in the month
- `17:18:30`   NAVI   Navient Corporation                  hy spread   836.8 bp, own volatility  63.9 ->  42.2% in 3 months,  18.4% in one | stress 95.1 | worse/down | stock -28.17% in the month
- `17:18:30`   GT     The Goodyear Tire & Rubber Company   hy spread   875.7 bp, own volatility  46.2 ->  42.2% in 3 months,  18.4% in one | stress 95.7 | worse/down | stock -13.99% in the month
- `17:18:30`   MPT    Medical Properties Trust, Inc.       hy spread   729.3 bp, own volatility  60.4 ->  42.2% in 3 months,  18.4% in one | stress 62.6 | flat/down | stock -15.11% in the month
- `17:18:30`   SABR   Sabre Corporation                    hy spread   836.4 bp, own volatility  60.9 ->  42.2% in 3 months,  18.4% in one | stress 34.6 | better/up | stock 3.09% in the month
- `17:18:30`   LUMN   Lumen Technologies, Inc.             hy spread   403.1 bp, own volatility  65.3 ->  36.4% in 3 months,  15.0% in one | stress 75.3 | flat/down | stock -23.77% in the month
- `17:18:30`   WHR    Whirlpool Corporation                hy spread   652.4 bp, own volatility  55.7 ->  36.4% in 3 months,  15.0% in one | stress 63.3 | flat/down | stock -14.26% in the month
- `17:18:30`   BHC    Bausch Health Companies Inc.         hy spread   592.0 bp, own volatility  66.1 ->  36.4% in 3 months,  15.0% in one | stress 59.7 | worse/up | stock 5.0% in the month
- `17:18:30`   GTN    Gray Media, Inc.                     hy spread   518.6 bp, own volatility  47.3 ->  36.4% in 3 months,  15.0% in one | stress 43.7 | better/up | stock 27.84% in the month
- `17:18:30`   CORZ   Core Scientific, Inc.                hy spread   460.3 bp, own volatility  89.7 ->  36.4% in 3 months,  15.0% in one | stress 51.4 | flat/down | stock -17.06% in the month
- `17:18:30` distressed: 130 companies (18 listed) | the nine cells today: {"worse/down": 122, "worse/flat": 17, "worse/up": 9, "flat/down": 367, "flat/flat": 271, "flat/up": 239, "better/down": 14, "better/flat": 18, "better/up": 45}
- `17:18:30` kept: ledger days 76 (was 76), dated holdings 65 (was 64), days of calls 1 (was 0); bond document 21106 rows under 4184 companies; history 4329 companies over 63 month-ends; track {"calls_kept": 1, "first_call": "2026-11-23", "last_call": "2026-11-23", "horizons": [{"key": "h1", "words": "1 month", "sessions": 21, "days_scored": 0, "stocks_scored": 0, "buckets": [], "next_due": {"call_day": "2026-11-23", "sessions_left": 21}}, {"key": "h3", "words": "3 months", "sessions": 63, "days_scored": 0, "stocks_scored": 0, "buckets": [], "next_due": {"call_day": "2026-11-23", "sessi
- `17:18:30` Oracle: {"book": "ig", "ccy": "USD", "spread_bp": 327.6, "stress": 99.6, "cell": "worse/down", "signal": "NONE", "pd_1y": 5.3, "due2y": 0.8, "px": 77.4, "px_min": 49.3, "curve_bp": 102.9, "vol": 68.0, "risk_1m": 9.2, "risk_3m": 31.3, "lo_3m": -21.8, "hi_3m": 21.5, "n_bonds": 56, "n_other": 0}
- `17:18:30` check the document is 3.0 on the v2 contract                                         ok
- `17:18:30` check the 2.0 and 2.1 columns are where they were, the 3.0 columns after them        ok
- `17:18:30` check every company that was on the board is still on it, in its book and its industry, tied as it was ok
- `17:18:30` check with the same holdings day, every company that was there has the spread it had ok
- `17:18:30` check the stress counts are the rows                                                 ok
- `17:18:30` check the AI stack's count is its rows and its layers                                ok
- `17:18:30` check no lead where a liquid CDS says the opposite                                   ok
- `17:18:30` check the counts are the rows                                                        ok
- `17:18:30` check it is the size the dry run inside Lambda saw                                   ok
- `17:18:30` check every key the first engine published is there                                  ok
- `17:18:30` check the model's names kept their fields                                            ok
- `17:18:30` check the run was not degraded                                                       ok
- `17:18:30` check a lot more companies: the non-dollar books are there, in their own books       ok
- `17:18:30` check the record was measured inside Lambda and is in the document                   ok
- `17:18:30` check the month-by-month document holds every month-end of the record                ok
- `17:18:30` check every chance of a fall is the record's own table, and no non-dollar company has one ok
- `17:18:30` check every bond is under its company, with no identifier                            ok
- `17:18:30` check the nine cells count the listed companies with a reading                       ok
- `17:18:30` check the day's calls were kept as they were said                                    ok
- `17:18:30` check the day's holdings were kept, so the history grows                             ok
- `17:18:30` check the track counts today's calls and has nothing to score yet                    ok
- `17:18:30` check three months is a real ledger day now                                          ok
- `17:18:30` check no ledger day, dated holdings or day of calls was removed                      ok
- `17:18:33` the site serves the three documents of this run: {"document": true, "bonds": true, "history": true}
- `17:18:33` ✅ 3.0.0 LIVE: 4184 companies (1107 listed; 1239 read in another currency), 130 distressed, the record of 63 month-ends measured inside Lambda in 146.2 s, 1083 listed companies with the chance of a fall it supports, every bond under its company; one dry run and one publication, both by hand, disclosed
