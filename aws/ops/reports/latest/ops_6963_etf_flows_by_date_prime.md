# ops 6963 - every ETF's flows by date: probe, check, bank (2026-11-26T03:32+00:00)

**Status:** success  
**Duration:** 847.1s  
**Finished:** 2026-11-26T03:46:51+00:00  

## Data

| funds_november | funds_on_newest_day | months | newest_day | rows_november | write | written |
|---|---|---|---|---|---|---|
| 5354 | 4976 | 2 | 2026-11-24 | 85000 | False | 0 |

## Log
## 1. A whole month for every fund, asked by processed date

- `03:32:57` 2026-10: 110000 rows in 23 pages, 12 s, complete True | 5164 funds | 23 effective days, 22 processed days | rows outside the month 0, repeated rows 0
- `03:32:57`     funds per effective day (last 8): {"2026-10-21": 5000, "2026-10-22": 4999, "2026-10-23": 5000, "2026-10-26": 5000, "2026-10-27": 5000, "2026-10-28": 5000, "2026-10-29": 5000, "2026-10-30": 4976}
- `03:32:57`     a row: {"processed_date": "2026-10-30", "effective_date": "2026-10-30", "composite_ticker": "ZVOL", "shares_outstanding": 3120000.0, "nav": 7.06, "fund_flow": 0.0} | fields {'processed_date': 2000, 'effective_date': 2000, 'composite_ticker': 2000, 'shares_outstanding': 2000, 'nav': 2000, 'fund_flow': 2000}
- `03:33:06` 2026-11: 85000 rows in 18 pages, 9 s, complete True | 5354 funds | 18 effective days, 17 processed days | rows outside the month 0, repeated rows 0
- `03:33:06`     funds per effective day (last 8): {"2026-11-13": 5000, "2026-11-16": 5001, "2026-11-17": 4999, "2026-11-18": 5001, "2026-11-19": 5000, "2026-11-20": 4999, "2026-11-23": 5000, "2026-11-24": 4976}
- `03:33:06`     a row: {"processed_date": "2026-11-24", "effective_date": "2026-11-24", "composite_ticker": "ZVOL", "shares_outstanding": 3600000.0, "nav": 5.74, "fund_flow": 0.0} | fields {'processed_date': 2000, 'effective_date': 2000, 'composite_ticker': 2000, 'shares_outstanding': 2000, 'nav': 2000, 'fund_flow': 2000}
## 2. The same rows in the bank's per-fund files

- `03:40:02` 2026-10: fund-days here 110000 | also in the per-fund files 108779, of which different 0 [] | per-fund fund-days of the month not here 14558 [["AAA", "2026-10-30"], ["AAA", "2026-10-29"], ["AAA", "2026-10-28"], ["AAA", "2026-10-27"], ["AAA", "2026-10-26"]] | funds here with no per-fund file 74
- `03:40:02`     newest day per fund, this answer against the per-fund file: {'older here': 5085, 'newer here': 75, 'same': 4}
- `03:40:02`     usable: True
- `03:46:51` 2026-11: fund-days here 84999 | also in the per-fund files 53447, of which different 7 [[["PRNT", "2026-11-20"], {"processed_date": "2026-11-20", "effective_date": "2026-11-20", "composite_ticker": "PRNT", "shares_outstanding": 2250000.0, "nav": 20.94676, "fund_flow": 0.0}, {"processed_date": "2026-11-20", "effective_date": "2026-11-20", "composite_ticker": "PRNT", "shares_outstanding": 2250000.0, "nav": 20.947, "fund_flow": 0.0}], [["NISM", "2026-11-20"], {"processed_date": "2026-11-20", "effective_da | per-fund fund-days of the month not here 5097 [["AAA", "2026-11-24"], ["AAA", "2026-11-23"], ["AAA", "2026-11-20"], ["AAA", "2026-11-19"], ["AAA", "2026-11-16"]] | funds here with no per-fund file 38
- `03:46:51`     newest day per fund, this answer against the per-fund file: {'same': 1480, 'newer here': 3871, 'older here': 3}
- `03:46:51`     usable: True
## 3. Banked where the bank's new dataset keeps them

- `03:46:51` data/warm/polygon-bank/etf-global-fund-flows-by-date/2026-10.json.gz: would be written (110000 rows, 16.5 MB) - probe only
- `03:46:51` data/warm/polygon-bank/etf-global-fund-flows-by-date/2026-11.json.gz: would be written (85000 rows, 12.7 MB) - probe only
- `03:46:51` ✅ CHECKED: November has 5354 funds, 4976 of them on the newest day 2026-11-24; the by-date answer equals the per-fund files
