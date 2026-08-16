# ops 4797 -- TE key wire, SFTR truth, DTCC hunt, v2.3 verify

**Status:** success  
**Duration:** 519.1s  
**Finished:** 2026-08-16T23:39:56+00:00  

## Data

| check | key_len | last_bp | value | waited_s |
|---|---|---|---|---|
| te_key_found | 31 |  | True |  |
| dtcc_inline_arrays |  |  | 0 |  |
| engine_completed |  |  | True | 511.6 |
| daily_btp_bund_sane |  | None | False |  |
| sftr_rows |  |  | 0 |  |
| board_total |  |  | 1634 |  |

## Log
## 1. TE key -> justhodl-repo env

- `23:31:21` ✅ TE_API_KEY present in justhodl-repo env
## 2. SFTR ground truth (openpyxl)

- `23:31:23` SFTR-Public-Data-EU-we-7-August-2026-110826.xlsx: sheets=['NEWT - EU', 'Outstanding - EU', 'Images - EU']
- `23:31:23`    row0: (None, None, None, None, None, 'SFTR Public Data\nfor week ending 07 August 2026', None, None, None, None, None)
- `23:31:23`    row1: (None, None, None, None, None, None, 'Cash Value (Eur mn)', 'Percentage', 'Number Of Transactions', 'Percentage', 'Collateral Market Value (Eur mn)*')
- `23:31:23`    row2: (None, 'ALL SFTS', None, None, None, None, None, None, None, None, None)
- `23:31:23`    row3: (None, None, None, 'Total SFT', None, None, 18886667.515700083, None, 1876008, None, 136814468.93901953)
- `23:31:23`    row4: (None, None, None, None, 'Total Repos', None, 18287729.42627817, 0.9682877834893833, 511023, 0.27239915821254496, 639315.182229086)
- `23:31:23`    row5: (None, None, None, None, None, 'Of which', None, None, None, None, None)
- `23:31:23` SFTR-Public-Data-UK-we-7-August-2026-110826.xlsx: sheets=['NEWT - UK', 'Outstanding - UK', 'Images - UK']
- `23:31:23`    row0: (None, None, None, None, None, 'SFTR Public Data\nfor week ending 07 August 2026', None, None, None, None, None)
- `23:31:23`    row1: (None, None, None, None, None, None, 'Cash Value (Eur mn)', 'Percentage', 'Number Of Transactions', 'Percentage', 'Collateral Market Value (Eur mn)*')
- `23:31:23`    row2: (None, 'ALL SFTS', None, None, None, None, None, None, None, None, None)
- `23:31:23`    row3: (None, None, None, 'Total SFT', None, None, 14337844.020492956, None, 1708820, None, 662802.414886613)
- `23:31:23`    row4: (None, None, None, None, 'Total Repos', None, 13603818.735941429, 0.9488050446425285, 401125, 0.2347380063435587, 226630.512307999)
- `23:31:23`    row5: (None, None, None, None, None, 'Of which', None, None, None, None, None)
## 3. DTCC inline data hunt

- `23:31:23`   xhr: https://fonts.googleapis.com/css2?family=Lato:wght@400;700&amp;display=swap
## 4. async v2.3 verify

- `23:39:56` ⚠ DE10Y_TE: ABSENT
- `23:39:56` ⚠ IT10Y_TE: ABSENT
- `23:39:56` ⚠ D_BTP_BUND_D: ABSENT
- `23:39:56` ⚠ WREPOFOR: ABSENT
