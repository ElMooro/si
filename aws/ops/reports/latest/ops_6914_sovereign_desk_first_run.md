# ops 6914 - the sovereign-stress desk document, first compile by the deployed engine, 2026-11-22T15:33+00:00

**Status:** success  
**Duration:** 61.8s  
**Finished:** 2026-11-22T15:34:42+00:00  

## Data

| checked_against_warehouse | countries | differ | failed_checks | measures | seconds |
|---|---|---|---|---|---|
| 58 | 30 | 0 | 0 | 176 | 61 |

## Log
## 1. Guard

- `15:33:40` justhodl-ciss-stress updated 2026-11-22T15:31:16 | as in this checkout: {'sovereign_desk.py': True, 'lambda_function.py': True} | timeout 900 s, memory 1024 MB
- `15:33:41` before: no desk document yet
## 2. One invoke, desk only

- `15:34:41` invoke: HTTP 200, function error None, 61s | {"sovereign": {"published": true, "generated_at": "2026-11-22T15:34:05.805934+00:00", "coverage": {"measures": 176, "countries": 30, "errors": 0, "status": "measured"}, "replay": {"manifest_key": "data/ciss-research/sovereign/runs/b5f96afde2fe6bf541a1fae8277fb38abf4977bb623de3fab0903ba574687535.json", "output_sha256": "5194fd273cdb78f09e35d4dec49765eaec572330b0f0593c555fb0ab02b069db", "compiler": {"key": "data/ciss-research/compilers/f77b876c0f066bbe91e2207455e5aea40eb879ad9e7149ba6690bbe193a152
## 3. Read back

- `15:34:42` desk 543437 bytes written 15:34:42 | history 2130284 bytes written 15:34:42
- `15:34:42` contract ciss-sovereign.v1 1.0.0 | generated 2026-11-22T15:34:05.805934+00:00 | coverage {"countries": 30, "errors": 0, "measures": 176, "status": "measured"} | errors {}
- `15:34:42` receipt b5f96afde2fe6bf5 | names this document: True | names this history: True | compiler stored: True
- `15:34:42` measures by group: {"auxiliary": 3, "ciss": 13, "clifs": 27, "contribution": 6, "curve": 4, "debt": 28, "euro_area": 7, "sovciss": 11, "spread": 27, "target": 22, "yield": 28}
- `15:34:42` 58 measures checked against their warehouse rows, 0 differ []
- `15:34:42` warehouse untouched: True (written 2026-11-22 07:14:26)
- `15:34:42`   ciss             61 series, - rows, newest -
- `15:34:42`   curve            4 series, 22712 rows, newest 2026-11-19
- `15:34:42`   yields           34 series, 12175 rows, newest 2026-10
- `15:34:42`   target           23 series, 4871 rows, newest 2026-09
- `15:34:42`   debt             34 series, 3998 rows, newest 2026-Q2
- `15:34:42`   eurusd           1 series, 7203 rows, newest 2026-11-20
- `15:34:42`   fx_claims_ea     1 series, 1455 rows, newest 2026-W46
- `15:34:42`   fx_claims_nonea  1 series, 1455 rows, newest 2026-W46
- `15:34:42` euro-area SovCISS 0.01123 on 2026-11-19 (lower than 76.5% of its history) | FR 0.17391 BE 0.16125 GR 0.10067 IT 0.0474 AT 0.04222 DE 0.03026 IE 0.02415 NL 0.01731 FI 0.00602 ES 0.00567 PT 0.00377 | all issuers minus AAA, 10y: 59.81 bp on 2026-11-19 | TARGET owed 1307.451 bn (2026-09-30)
- `15:34:42` ✅ the deployed engine compiled and published the sovereign-stress desk document; it replays, agrees with the warehouse, and the warehouse was not touched
