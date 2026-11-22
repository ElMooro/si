# ops 6914 - the sovereign-stress desk document, first compile by the deployed engine, 2026-11-22T16:39+00:00

**Status:** success  
**Duration:** 92.0s  
**Finished:** 2026-11-22T16:40:39+00:00  

## Data

| checked_against_warehouse | countries | differ | failed_checks | measures | seconds |
|---|---|---|---|---|---|
| 58 | 30 | 0 | 0 | 266 | 90 |

## Log
## 1. Guard

- `16:39:07` justhodl-ciss-stress updated 2026-11-22T16:38:02 | as in this checkout: {'sovereign_desk.py': True, 'lambda_function.py': True} | timeout 900 s, memory 1024 MB
- `16:39:08` before: a desk document written 2026-11-22 15:34 exists
## 2. One invoke, desk only

- `16:40:38` invoke: HTTP 200, function error None, 90s | {"sovereign": {"published": true, "generated_at": "2026-11-22T16:39:59.412623+00:00", "coverage": {"measures": 266, "countries": 30, "errors": 0, "status": "measured"}, "replay": {"manifest_key": "data/ciss-research/sovereign/runs/2e6dabb0b2ea8e2ec25646efa4dab81de8a769b228d85ea2404142894cc711f3.json", "output_sha256": "ca0f55d41f78fcd92487e5cba46c28a842bc1857525d962df798d5cf05c055a7", "compiler": {"key": "data/ciss-research/compilers/a2d75bd16f44e0b67a1630bc5d016fec9e8cd2dd0e4bbe2a24b78daa6e9e91
## 3. Read back

- `16:40:38` desk 820800 bytes written 16:40:39 | history 2333035 bytes written 16:40:39
- `16:40:38` contract ciss-sovereign.v1 1.1.0 | generated 2026-11-22T16:39:59.412623+00:00 | coverage {"countries": 30, "errors": 0, "measures": 266, "status": "measured"} | errors {}
- `16:40:39` receipt 2e6dabb0b2ea8e2e | names this document: True | names this history: True | compiler stored: True
- `16:40:39` measures by group: {"auxiliary": 3, "ciss": 13, "clifs": 27, "contribution": 6, "curve": 4, "debt": 28, "deficit": 28, "euro_area": 7, "interest": 28, "primary": 28, "sovciss": 11, "sovciss_ended": 6, "spread": 27, "target": 22, "yield": 28}
- `16:40:39` 58 measures checked against their warehouse rows, 0 differ []
- `16:40:39` warehouse untouched: True (written 2026-11-22 07:14:26)
- `16:40:39`   ciss             67 series, - rows, newest -
- `16:40:39`   curve            4 series, 22712 rows, newest 2026-11-19
- `16:40:39`   yields           34 series, 12175 rows, newest 2026-10
- `16:40:39`   target           23 series, 4871 rows, newest 2026-09
- `16:40:39`   debt             34 series, 3998 rows, newest 2026-Q2
- `16:40:39`   deficit          31 series, 3356 rows, newest 2026-Q2
- `16:40:39`   primary          31 series, 3356 rows, newest 2026-Q2
- `16:40:39`   interest         31 series, 3356 rows, newest 2026-Q2
- `16:40:39`   eurusd           1 series, 7203 rows, newest 2026-11-20
- `16:40:39`   fx_claims_ea     1 series, 1455 rows, newest 2026-W46
- `16:40:39`   fx_claims_nonea  1 series, 1455 rows, newest 2026-W46
- `16:40:39` euro-area SovCISS 0.01123 on 2026-11-19 (lower than 76.5% of its history) | FR 0.17391 BE 0.16125 GR 0.10067 IT 0.0474 AT 0.04222 DE 0.03026 IE 0.02415 NL 0.01731 FI 0.00602 ES 0.00567 PT 0.00377 | all issuers minus AAA, 10y: 59.81 bp on 2026-11-19 | TARGET owed 1307.451 bn (2026-09-30)
- `16:40:39` ✅ the deployed engine compiled and published the sovereign-stress desk document; it replays, agrees with the warehouse, and the warehouse was not touched
