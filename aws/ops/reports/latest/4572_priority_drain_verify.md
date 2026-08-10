# ops 4572 — priority drain v2 verify

**Status:** success  
**Duration:** 981.5s  
**Finished:** 2026-08-10T00:26:30+00:00  

## Data

| last_pop_drained | next_popularity | phase2 | pre_cats | pre_imported | pre_updated_at | queue_cursor | queue_total | rate_rpm | reserved_concurrency | status | throttled_429 |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  | 1 |  |  |
|  |  |  | 81 | 11859 | 2026-08-10T00:03:47+00:00 |  |  |  |  |  |  |
| None | None | None |  |  |  | None | None | None |  | walking | None |

## Log
## 1. settle by marker

- `00:10:09` ✅ marker+config live after 0s (mem=2048, timeout=850)
## 2. single-flight + key belts

- `00:10:10` ✅ String mirror param set
- `00:10:16` ✅ env belt injected (runner-side; public config stays keyless)
## 3. pause kill-switch probe

- `00:10:19` ✅ paused probe -> {"skipped": "paused", "categories_done": 0, "series_seen": 0, "series_excluded_stale": 0, "series_imported": 0, "status"
## 4. kick the walk + observe

- `00:26:30` {"at": "2026-08-10T00:08:47+00:00", "phase2": null, "cats": 81, "imported": 11927, "cursor": null, "qtotal": null, "rpm": null, "last_pop": null, "next_pop": null, "blocked": null}
## 5. ledger order proof

- `00:26:30` ⚠ drain not reached in window — discovery still walking; chain will carry it (not a gate fail if discovery advanced)
- `00:26:30` ✗ FAIL: discovery made no category progress | rate_rpm 0 out of bounds
