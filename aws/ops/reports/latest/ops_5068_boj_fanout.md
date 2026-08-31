## P0 real coverage (done / len(codes))

**Status:** success  
**Duration:** 2107.9s  
**Finished:** 2026-08-31T00:52:43+00:00  

## Data

| dbs | host | rows | series |
|---|---|---|---|
| 22 | benzinga-news-agent-warm | 288964 | 59765/120394 |

## Log
- `00:17:36`   code fresh 2026-08-31T00:17:34.000+0000
- `00:17:36`   dbs with state: 16   series 55,306 / 85,102  (65.0%)
- `00:17:36`   rows banked 258,846 across 7,148 part files
- `00:17:36`   the page reports 55,306/120,394 -- so ~35,292 series are in dbs that have NO state file at all
- `00:17:36`   dbs short (2), worst first:
- `00:17:36`     FF             10,140 / 33,887    23,747 outstanding
- `00:17:36`     BP01           11,940 / 17,989    6,049 outstanding
## P1 give it a trigger

- `00:17:37`   existing rules: NONE
- `00:17:37`   fanout target on benzinga-news-agent-warm (rate(5 minutes)) failed=0
## P2 fan out, correct payload

- `00:17:39`   fanout -> b'{"ok": true, "mode": "fanout", "dbs": 22, "invoked": 22}'
- `00:29:20`   t+12min  series 57,787/120,394 (+2,481)  rows 276,714 (+17,868)  dbs=22
- `00:41:01`   t+23min  series 58,678/120,394 (+3,372)  rows 282,012 (+23,166)  dbs=22
- `00:52:42`   t+35min  series 59,765/120,394 (+4,459)  rows 288,964 (+30,118)  dbs=22
- `00:52:43`   drained 4,459 series in 35 min (127/min); rows +30,118; dbs 16 -> 22
- `00:52:43`   60,629 series left in known dbs -> ~7.9 h at this rate
## P3 what is still untouched

- `00:52:43`   universe dbs=22  with state=22  never started: none
- `00:52:43`   -> data/ops/boj-expedite.json
- `00:52:43` ops 5068 GREEN -- BOJ fanned out and triggered
