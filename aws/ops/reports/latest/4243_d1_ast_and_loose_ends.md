# ops 4243 — AST-based D1, and the loose ends

**Status:** failure  
**Duration:** 194.3s  
**Finished:** 2026-08-01T15:37:51+00:00  

## Error

```
SystemExit: FAILS: justhodl-fundamental-census classified BOUNDED_FLAG, expected BOUNDED_COUNTER
```

## Data

| cached | case | complete | detail | env_writers | expect | failed | function | got | key | modified | objects | passed | repo_refs | scanned | section | size | total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | unguarded |  |  |  | UNGUARDED |  |  | UNGUARDED |  |  |  | True |  |  | selftest |  |  |
|  | counter |  |  |  | BOUNDED_COUNTER |  |  | BOUNDED_COUNTER |  |  |  | True |  |  | selftest |  |  |
|  | kickoff |  |  |  | BOUNDED_FLAG |  |  | BOUNDED_FLAG |  |  |  | True |  |  | selftest |  |  |
|  | clean |  |  |  | NO_SELF_INVOKE |  |  | NO_SELF_INVOKE |  |  |  | True |  |  | selftest |  |  |
| 0 |  | True |  |  |  | 23 |  |  |  |  |  |  |  | 745 | d1scan |  | 768 |
|  |  |  | {"bound": 10} |  | BOUNDED_COUNTER |  | justhodl-13f-clone-alpha | BOUNDED_COUNTER |  |  |  | True |  |  | real_engines |  |  |
|  |  |  | {"flag": "_internal"} |  | BOUNDED_FLAG |  | justhodl-equity-research | BOUNDED_FLAG |  |  |  | True |  |  | real_engines |  |  |
|  |  |  | {"flag": "phase"} |  | BOUNDED_COUNTER |  | justhodl-fundamental-census | BOUNDED_FLAG |  |  |  | False |  |  | real_engines |  |  |
|  |  |  |  |  |  |  |  |  | backups/2026-07-12/lambda-fleet-config.json.gz | 2026-07-12 18:09:23 |  |  |  |  | stale_bucket | 17781 |  |
|  |  |  |  | none |  |  |  |  |  |  | 1 |  | ./aws/ops/ran/ops_4239_converge_and_crr.py
./aws/ops/ran/ops_3155_presaas_backup.py |  | stale_bucket |  |  |

## Log
## 1. Deploy fleet-integrity v1.1.0

- `15:34:53` ✅ marker verified
## 2. GATE A — classifier self-test (must be 4/4)

- `15:34:54` ✅    unguarded  expect=UNGUARDED        got=UNGUARDED        {}
- `15:34:54` ✅    counter    expect=BOUNDED_COUNTER  got=BOUNDED_COUNTER  {'bound': 10}
- `15:34:54` ✅    kickoff    expect=BOUNDED_FLAG     got=BOUNDED_FLAG     {'flag': '_internal'}
- `15:34:54` ✅    clean      expect=NO_SELF_INVOKE   got=NO_SELF_INVOKE   {}
- `15:34:54` ✅ 4/4 — one true positive, three no-false-positives
## 3. Scan the fleet (incremental, sha-cached)

- `15:37:27` pass 1 -> {"ok": true, "mode": "d1scan", "scanned": 745, "from_cache": 0, "failed": 23, "cursor": 0, "total": 768, "complete": true, "cache_entries": 745}
## 4. GATE B — the three real engines classify correctly

- `15:37:28` ✅    justhodl-13f-clone-alpha           expect=BOUNDED_COUNTER  got=BOUNDED_COUNTER  {'bound': 10}
- `15:37:28` ✅    justhodl-equity-research           expect=BOUNDED_FLAG     got=BOUNDED_FLAG     {'flag': '_internal'}
- `15:37:28` ✗    justhodl-fundamental-census        expect=BOUNDED_COUNTER  got=BOUNDED_FLAG     {'flag': 'phase'}
- `15:37:28` fleet classification: {'NO_SELF_INVOKE': 742, 'BOUNDED_COUNTER': 1, 'BOUNDED_FLAG': 2}
- `15:37:28` GENUINELY UNGUARDED self-invokers: 0
- `15:37:28`    bounded  justhodl-13f-clone-alpha               bound=10
## 5. Schedule the daily scan + declare it

- `15:37:29` ✅ cron(0 5 * * ? *) -> justhodl-fleet-integrity mode=d1scan (1 target)
- `15:37:29` ✅ manifest written to config/ (staged by run-ops.yml as of this session) — 449 rules
- `15:37:43` ✅ reconciler drift = 0
## 6. LOOSE END A — identify the stale backup bucket

- `15:37:44` justhodl-backups-857687956942 holds 1 object(s)
- `15:37:44`    backups/2026-07-12/lambda-fleet-config.json.gz                17781 bytes  2026-07-12 18:09:23
- `15:37:51` functions whose ENV names this bucket: NONE
- `15:37:51` repo files naming this bucket: ./aws/ops/ran/ops_4239_converge_and_crr.py
./aws/ops/ran/ops_3155_presaas_backup.py
- `15:37:51` ⚠ NOT DELETED. Identified only — 1 object(s), 0 env writer(s), 1 repo reference(s). Deleting a backup bucket on a hunch is the one mistake with no undo.
## RESULT

- `15:37:51` ✗   justhodl-fundamental-census classified BOUNDED_FLAG, expected BOUNDED_COUNTER
