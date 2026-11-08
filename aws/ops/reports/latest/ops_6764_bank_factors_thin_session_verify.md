# ops 6764 - bank-factors 1.0.8 verify + heal today's book and the leaders desk (2026-11-08)

**Status:** success  
**Duration:** 499.4s  
**Finished:** 2026-11-08T11:46:52+00:00  

## Data

| artifact_version | book_returns | book_tape | book_version | leader_candidates | leaders_as_of | leaders_n |
|---|---|---|---|---|---|---|
| 1.0.8 | 12063 | 2026-11-06 | 1.0.8 | 5166 | 2026-11-07 | 670 |

## Log
## G0. holds and the three-rung ladder

- `11:38:34` governor stage NORMAL | capped ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi'] | anomaly []
- `11:38:34` justhodl-bank-factors: State Active / Successful, last modified 2026-11-08T11:37:34.000+0000, timeout 900s
- `11:38:34` artifact: 1296760 B, lambda_function.py VERSION 1.0.8, marker 'thin_skipped' present
## 1. bank-factors: invoke (Event) and poll the manifest

- `11:38:34` before: generated 2026-11-08T02:50:45.044248+00:00 | as_of polygon-tape 2026-11-07 | returns 266
- `11:38:34` invoke status 202
- `11:46:06` after: generated 2026-11-08T11:38:35.707920+00:00 | version 1.0.8 | as_of polygon-tape 2026-11-06 | returns coverage 12063 | returns info {"sessions": 253, "last_session": "2026-11-06", "symbols": 12561, "seconds": 36.9, "thin_floor": 3148.25, "thin_skipped": [{"day": "2026-11-07", "rows": 271}]}
- `11:46:06` leaders funnel over the healed matrix: 27109 rows, 5166 candidates (Sunday 02:50 book: 153)
## 2. leaders: invoke (Event) and poll data/leaders.json

- `11:46:06` leaders.json before: 2026-11-07T03:50:39+00:00
- `11:46:06` invoke status 202
- `11:46:52` leaders.json after: as_of 2026-11-07 | 670 leaders / 145 industries / 109 funds | bank last session 2026-11-07 | errors 0
- `11:46:52` ✅ bank-factors 1.0.8 live; the book and the leaders desk are whole again
