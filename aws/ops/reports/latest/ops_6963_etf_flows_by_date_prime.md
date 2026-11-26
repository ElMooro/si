# ops 6963 - every ETF's flows by date: probe, check, bank (2026-11-26T03:57+00:00)

**Status:** success  
**Duration:** 48.7s  
**Finished:** 2026-11-26T03:58:12+00:00  

## Data

| funds_november | funds_on_newest_day | months | newest_day | per_fund_files_on_newest_day | rows_november | write | written |
|---|---|---|---|---|---|---|---|
| 5794 | 5707 | 2 | 2026-11-24 | 1628 | 96095 | True | 2 |

## Log
## 1. The bank's per-fund files (their newest 45 rows each)

- `03:57:43` 5789 funds, 255359 fund-days
## 2. Each month asked by processed date, funds A to Z (the order that returns every row), checked against the per-fund files

- `03:57:58` 2026-10: 124639 rows in 25 pages, 15 s, complete True | 5772 funds, 23 days, per day (last 4) {"2026-10-27": 5679, "2026-10-28": 5708, "2026-10-29": 5637, "2026-10-30": 5615} | of the per-fund files' 123313 fund-days it has 123313, different 0 [] | outside the month 0 | usable True
- `03:58:10` 2026-11: 96095 rows in 20 pages, 12 s, complete True | 5794 funds, 18 days, per day (last 4) {"2026-11-19": 5407, "2026-11-20": 5401, "2026-11-23": 5727, "2026-11-24": 5707} | of the per-fund files' 58547 fund-days it has 58543, different 4 [[["ARKD", "2026-11-06"], 0.0, 0.0, "2026-11-06", "2026-11-06"], [["ARKG", "2026-11-16"], 0.0, 30207156.0, "2026-11-17", "2026-11-16"], [["ARKI", "2026-11-06"], 0.0, 0.0, "2026-11-06", "2026-11-06"]] | outside the month 0 | usable True
## 3. Banked where the bank's new dataset keeps them

- `03:58:12` data/warm/polygon-bank/etf-global-fund-flows-by-date/2026-10.json.gz: written, 124639 rows, 1.2 MB stored (18.7 MB as text); read back 124639 rows, equal True
- `03:58:12` data/warm/polygon-bank/etf-global-fund-flows-by-date/2026-11.json.gz: written, 96095 rows, 1.0 MB stored (14.4 MB as text); read back 96095 rows, equal True
- `03:58:12` ✅ BANKED: November has 5794 funds, 5707 of them on the newest day 2026-11-24 (the per-fund files: 1628)
