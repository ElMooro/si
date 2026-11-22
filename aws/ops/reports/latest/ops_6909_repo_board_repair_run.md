# ops 6909 - the repo board rebuilt after FRED's refusal, 2026-11-22T06:37+00:00

**Status:** success  
**Duration:** 224.6s  
**Finished:** 2026-11-22T06:40:59+00:00  

## Data

| fred_group | groups | inventory | run_seconds | series |
|---|---|---|---|---|
| 1028 | 21 | fred | 224 | 1803 |

## Log
- `06:37:15` justhodl-repo updated 2026-11-22T06:36:29 | modules as in this checkout: {'repo_sources.py': True, 'repo_scope.py': True} | the handler calls the inventory check: True | timeout 900 s
- `06:37:15` before: written 2026-11-22 06:12 | engine 3.1 | 20 groups, 766 series
- `06:40:59` invoke: HTTP 200, 224 s | {"ok": true, "v": "3.1", "ice": 192, "frepo": 1028, "series": 1803, "skipped": 7, "history_written": 5, "history_unchanged": 1798, "barometer": 52.2, "label": "NORMAL", "secs": 222.9}
- `06:40:59` after: written 2026-11-22 06:41 | engine 3.1 | 21 groups, 1803 series | counts {"series": 1803, "skipped": 7, "history_files": 1803, "history_written": 5, "history_unchanged": 1798}
- `06:40:59` FRED group: 1028 series | inventory: {"ids": 1028, "inventory": "fred"} | own sources: {"survey_rate": "banked", "haircuts": "level"}
- `06:40:59`   SOFR             2156 obs, 2018-04-03 to 2026-11-19: 3.91
- `06:40:59`   IORB             1944 obs, 2021-07-29 to 2026-11-23: 3.9
- `06:40:59`   RRPONTSYAWARD    3207 obs, 2013-09-23 to 2026-11-20: 3.75
- `06:40:59`   DFEDTARU         6550 obs, 2008-12-16 to 2026-11-21: 4.0
- `06:40:59`   DFEDTARL         6550 obs, 2008-12-16 to 2026-11-21: 3.75
- `06:40:59`   DFEDTAR          9577 obs, 1982-09-27 to 2008-12-15: 1.0
- `06:40:59`   WRESBAL          1249 obs, 2002-12-18 to 2026-11-18: 2979656.0
- `06:40:59`   GC_SURVEY        4998 obs, 1998-02-20 to 2018-02-28: 1.4
- `06:40:59`   D_SOFR_ADMIN     2156 obs, 2018-04-03 to 2026-11-19: 1.000000000000023
- `06:40:59`   D_EFFR_TARGET    16125 obs, 1982-09-27 to 2026-11-19: -12.00000000000001
- `06:40:59`   D_REPO_TARGET    7154 obs, 1998-02-20 to 2026-11-19: 3.500000000000014
- `06:40:59`   D_TARGET_REPO    7154 obs, 1998-02-20 to 2026-11-19: -3.500000000000014
- `06:40:59` recorded inventory: 1005 ids at 2026-11-22T06:37:16+00:00
- `06:40:59` ✅ the board is whole again: 1803 series in 21 groups, the FRED group asked of FRED itself
