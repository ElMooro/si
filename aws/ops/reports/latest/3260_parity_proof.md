# ops 3260 — id-parity + ranker fusion proven

**Status:** failure  
**Duration:** 3.4s  
**Finished:** 2026-07-13T14:04:41+00:00  

## Error

```
SystemExit: 1
```

## Data

| brain_ids | mirror | mirror_ids_in_brain | missing_before | n_fails | n_warns | put_failed | rankers_proven | still_missing | upserted | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 12122 | 3322 |  | 403 |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 403 |  |  | 0 |  |
|  |  | 2919 |  |  |  |  |  | 403 |  |  |
|  |  |  |  |  |  |  | 2 |  |  |  |
|  |  |  |  | 1 | 2 |  |  |  |  | FAIL |

## Log
## 1. Parity by ID

- `14:04:39`     PUT err: HTTP Error 400: Bad Request
- `14:04:39`     PUT err: HTTP Error 400: Bad Request
- `14:04:39`     PUT err: HTTP Error 400: Bad Request
## 2. khalid_note riding the rankers — proven

- `14:04:41` ✅ data/best-setups.json: 74 rows carry khalid_note (14 non-null) — e.g. NVDA stance=BEARISH
- `14:04:41` ✅ data/alpha-compass.json: 3 rows carry khalid_note (0 non-null) — e.g. NEM stance=None
- `14:04:41` ⚠ data/master-rank.json: khalid_note not found anywhere in feed
- `14:04:41` ⚠ some ranker feeds pre-date the join — next scheduled run refreshes them
- `14:04:41` ✗ 403 mirror notes still absent from brain
