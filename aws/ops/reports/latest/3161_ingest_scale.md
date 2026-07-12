# ops 3161 — ingest at harvest scale

**Status:** success  
**Duration:** 450.4s  
**Finished:** 2026-07-12T20:15:30+00:00  

## Error

```
SystemExit: 0
```

## Data

| brain_error | brain_failed | brain_upserted | burst_secs | burst_status | memory | mirror_added | n_fails | n_warns | projected_1983_notes_secs | timeout | verdict | wl_only_status | wl_saved | wl_secs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 1024 |  |  |  |  | 300 |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 200 | 2 | 1.2 |
| None | 0 | 300 | 4.0 | 200 |  | 300 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 26.4 |  |  |  |  |  |
|  |  |  |  |  |  |  | 0 | 0 |  |  | PASS |  |  |  |

## Log
## 1. Deploy hardened ingest (1024MB / 300s / parallel)

- `20:08:00`   zip: 56091 bytes
## 1. Lambda

- `20:08:00`   Lambda exists — updating
- `20:08:06` ✅   ✓ updated justhodl-tv-notes-ingest
## 2. Watchlists-first request (the new order)

- `20:08:08` ✅ watchlists land independently of notes (the fix that saves them when a note chunk dies)
## 3. 300-note burst — the scale that broke it

- `20:08:12` ✅ 300 notes accepted in 4.0s (brain 300, mirror 300)
## 4. Cleanup e2e artifacts

- `20:15:29` probe notes deleted: 300/300
- `20:15:30` ✅ e2e watchlists stripped — clean slate for the real sync
