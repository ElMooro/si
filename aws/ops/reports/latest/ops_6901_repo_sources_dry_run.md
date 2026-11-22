# ops 6901 - the repo board's new source readers, dry run from the checkout, 2026-11-22T04:07+00:00

**Status:** success  
**Duration:** 7.5s  
**Finished:** 2026-11-22T04:07:37+00:00  

## Data

| dtcc_agency_new_days | dtcc_treasury_new_days | haircut_forced | haircut_normal | survey_n |
|---|---|---|---|---|
| 51 | 51 | 73 written, +219 obs, 0 revised | 73 written, +219 obs, 0 revised | 4998 |

## Log
## 1. Primary dealer survey rate, 1998-2018

- `04:07:30` bank_survey_rate: {"status": "written", "n": 4998, "first": "1998-02-20", "last": "2018-02-28"} | would write [('data/warm/nyfed-research/gc-survey-rate.json', 4998, '0b2da5f73587dc12')]
- `04:07:30` 4998 observations 1998-02-20 to 2018-02-28 | min -0.01 max 6.60 | years 1998:213 1999:250 2000:251 2001:245 2002:249 2003:250 2004:250 2005:250 2006:250 2007:250 2008:251 2009:250 2010:250 2011:250 2012:249 2013:250 2014:250 2015:250 2016:250 2017:250 2018:40
- `04:07:30`   1998-08-17  5.74
- `04:07:30`   1998-10-08  5.09
- `04:07:30`   1998-12-31  4.81
- `04:07:30`   1999-12-31  2.87
- `04:07:30`   2001-09-10  3.46
- `04:07:30`   2001-09-13  None
- `04:07:30`   2001-09-18  2.19
- `04:07:30`   2003-06-25  0.88
- `04:07:30`   2007-08-09  5.28
- `04:07:30`   2007-08-20  4.21
- `04:07:30`   2008-03-14  2.77
- `04:07:30`   2008-03-17  2.0
- `04:07:30`   2008-09-12  2.2
- `04:07:30`   2008-09-15  1.66
- `04:07:30`   2008-09-16  1.03
- `04:07:30`   2008-09-17  0.25
- `04:07:30`   2008-09-18  0.76
- `04:07:30`   2008-09-30  0.33
- `04:07:30`   2008-10-01  0.76
- `04:07:30`   2008-12-16  0.1
- `04:07:30`   2008-12-31  0.03
- `04:07:30`   2010-11-30  0.24
- `04:07:30`   2014-08-22  0.06
- `04:07:30`   2016-12-30  0.49
- `04:07:30`   2018-02-28  1.4
## 2. Tri-party haircuts

- `04:07:30` old workbook: 3897 rows, 180 snapshots 2010-05-11 to 2025-04-09, 23 groups | new workbook: 529 rows, 23 snapshots 2024-12-10 to 2026-10-09, 23 groups
- `04:07:30` snapshots in both workbooks: 5 (2024-12-10 ... 2025-04-09) | last 6 of the new one: ['2026-05-11', '2026-06-09', '2026-07-09', '2026-08-11', '2026-09-10', '2026-10-09']
- `04:07:30` groups only in the old: ['CDOs', 'Whole Loans'] | only in the new: ['Federal Reserve', 'Sponsored GC']
- `04:07:31` banked ids 81 | parse makes 81 ids | banked ids the parse does not make: [] | ids the parse makes that are not banked: []
- `04:07:36` bank against old  workbook(s): 12102 observations equal, 240 different, 1115 only in the bank, 0 only in the source
- `04:07:36` bank against new  workbook(s): 1460 observations equal, 0 different, 11997 only in the bank, 219 only in the source
- `04:07:36` bank against both workbook(s): 13457 observations equal, 0 different, 0 only in the bank, 219 only in the source
- `04:07:36` months both workbooks cover, per banked observation: {"equals_old": 0, "equals_new": 240, "equals_both": 105, "equals_neither": 0}
- `04:07:36`   abs-investment-grade--collateral_value 2025-04-09: bank 19.56 | old workbook 23.07 | new workbook 19.56
- `04:07:36`   abs-investment-grade--collateral_value 2024-12-10: bank 20.28 | old workbook 24.29 | new workbook 20.28
- `04:07:36`   abs-investment-grade--collateral_value 2025-03-11: bank 20.54 | old workbook 24.64 | new workbook 20.54
- `04:07:36`   abs-investment-grade--collateral_value 2025-01-10: bank 19.31 | old workbook 23.63 | new workbook 19.31
- `04:07:36`   abs-investment-grade--collateral_value 2025-02-11: bank 21.01 | old workbook 25.34 | new workbook 21.01
- `04:07:36`   abs-investment-grade--top3_concentration 2025-04-09: bank 53.0 | old workbook 46.0 | new workbook 53.0
- `04:07:36`   abs-investment-grade--top3_concentration 2024-12-10: bank 47.3 | old workbook 40.1 | new workbook 47.3
- `04:07:36`   abs-investment-grade--top3_concentration 2025-03-11: bank 49.2 | old workbook 42.9 | new workbook 49.2
- `04:07:36`   abs-investment-grade--top3_concentration 2025-01-10: bank 44.2 | old workbook 37.7 | new workbook 44.2
- `04:07:36`   abs-investment-grade--top3_concentration 2025-02-11: bank 49.0 | old workbook 41.9 | new workbook 49.0
- `04:07:36` banked titles the parse reproduces: 81 of 81
- `04:07:36` refresh_haircuts dry run, as the engine runs it: {"status": "would write", "asked": true, "source_newest": "2026-10-09", "bank_newest_before": "2026-07-09", "written": 73, "unchanged": 0, "new_series": [], "observations_added": 219, "observations_revised": 0, "dry_run": true, "old_workbook_read": false, "series": 81, "bank_newest_after": "2026-10-09"}
- `04:07:36` refresh_haircuts dry run, forced (both workbooks): {"status": "would write", "asked": true, "source_newest": "2026-10-09", "bank_newest_before": "2026-07-09", "written": 73, "unchanged": 8, "new_series": [], "observations_added": 219, "observations_revised": 0, "dry_run": true, "old_workbook_read": true, "series": 81, "bank_newest_after": "2026-10-09"}
- `04:07:36`   us-treasuries-excluding-strips--median_haircut: 2026-06-09 2.0, 2026-07-09 2.0, 2026-08-11 2.0, 2026-09-10 2.0, 2026-10-09 2.0
- `04:07:36`   agency-mbs--median_haircut: 2026-06-09 2.0, 2026-07-09 2.0, 2026-08-11 2.0, 2026-09-10 2.0, 2026-10-09 2.0
- `04:07:36`   equities--median_haircut: 2026-06-09 8.0, 2026-07-09 8.0, 2026-08-11 8.0, 2026-09-10 8.0, 2026-10-09 8.0
- `04:07:36`   corporates-non-investment-grade--median_haircut: 2026-06-09 8.0, 2026-07-09 8.0, 2026-08-11 8.0, 2026-09-10 9.2, 2026-10-09 8.0
- `04:07:36`   total--collateral_value: 2026-06-09 4298.12, 2026-07-09 4400.7, 2026-08-11 4285.11, 2026-09-10 4221.62, 2026-10-09 4232.29
- `04:07:36`   sponsored-gc--collateral_value: 2026-06-09 810.42, 2026-07-09 865.63, 2026-08-11 788.28, 2026-09-10 786.86, 2026-10-09 820.78
## 3. DTCC daily fails

- `04:07:36` https://cms-prod.dtcc.com/data/failsdata.csv: 250 days 2025-11-20 to 2026-11-20 | last three Treasury: 2026-11-18 $24.1B, 2026-11-19 $23.0B, 2026-11-20 $16.4B
- `04:07:37`   treasury: bank 266 days to 2026-09-08 | shared days equal 199, different 0 | new days 51 | merged 317 days 2025-08-14 to 2026-11-20 | gaps longer than 5 days: []
- `04:07:37`   agency: bank 266 days to 2026-09-08 | shared days equal 199, different 0 | new days 51 | merged 317 days 2025-08-14 to 2026-11-20 | gaps longer than 5 days: []
- `04:07:37` ✅ dry run; nothing written
