# ops 6963 - every ETF's flows by date: probe, check, bank (2026-11-26T03:48+00:00)

**Status:** success  
**Duration:** 50.4s  
**Finished:** 2026-11-26T03:49:44+00:00  

## Data

| one_day_whole | whole | write |
|---|---|---|
| True | {'month, newest date first': False, 'month, oldest date first': False, 'month, by fund A to Z': False, 'month, by fund Z to A': False} | False |

## Log
## 1. The bank's per-fund files (their newest 45 rows each)

- `03:49:14` 5789 funds, 255359 fund-days
## 2. Which way of asking by date returns every fund

- `03:49:21` month, newest date first (18 pages, 7 s, complete True): 85000 rows, 5354 funds, 18 days | per day (last 4) {"2026-11-19": 5000, "2026-11-20": 4999, "2026-11-23": 5000, "2026-11-24": 4976} | of the per-fund files' 58520 fund-days it has 53423 (91.3%), different 0 | first and last fund BBSC .. ZVOL
- `03:49:28` month, oldest date first (18 pages, 7 s, complete True): 85000 rows, 5349 funds, 18 days | per day (last 4) {"2026-11-19": 5000, "2026-11-20": 4999, "2026-11-23": 5000, "2026-11-24": 4976} | of the per-fund files' 58520 fund-days it has 47478 (81.1%), different 4 | first and last fund AAA .. VO
- `03:49:35` month, by fund A to Z (20 pages, 7 s, complete True): 96095 rows, 5794 funds, 18 days | per day (last 4) {"2026-11-19": 5407, "2026-11-20": 5401, "2026-11-23": 5727, "2026-11-24": 5707} | of the per-fund files' 58520 fund-days it has 58516 (100.0%), different 4 | first and last fund AAA .. ZVOL
- `03:49:43` month, by fund Z to A (20 pages, 7 s, complete True): 96095 rows, 5794 funds, 18 days | per day (last 4) {"2026-11-19": 5407, "2026-11-20": 5401, "2026-11-23": 5727, "2026-11-24": 5707} | of the per-fund files' 58520 fund-days it has 58516 (100.0%), different 4 | first and last fund AAA .. ZVOL
- `03:49:44` one day 2026-11-24, by fund A to Z (2 pages, 1 s, complete True): 5734 rows, 5734 funds, 2 days | per day (last 4) {"2026-11-23": 27, "2026-11-24": 5707} | of the per-fund files' 1628 fund-days it has 1628 (100.0%), different 0 | first and last fund AAA .. ZVOL
- `03:49:44` A-to-Z and Z-to-A together: 96091 fund-days; of the per-fund files' 58520 it has 58516
- `03:49:44` newest-first and oldest-first together: 96091 fund-days; of the per-fund files' 58520 it has 58516
- `03:49:44` funds on the newest day 2026-11-24: A-to-Z 5707, Z-to-A 5707, together 5707, one-day question 5734, per-fund files 1628
- `03:49:44` ✅ PROBE COMPLETE: nothing was banked
