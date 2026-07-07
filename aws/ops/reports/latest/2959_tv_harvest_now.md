## 1. Invoke justhodl-tv-notes-crawler

**Status:** failure  
**Duration:** 21.8s  
**Finished:** 2026-07-07T04:03:17+00:00  

## Error

```
SystemExit: 1
```

## Data

| brain_errors | brain_upserted | crawler_ok | elapsed_seconds | mirror_count | mirror_updated | notes_harvested | notes_in_mirror | session_valid | summary | symbols_covered | username |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | False | 21.3 |  |  | 0 | 0 | False |  | 0 | None |
|  |  |  |  | 0 | 2026-07-07T04:03:16.577282+00:00 |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | harvest: session_valid=False notes=0 tickers=0 mirror=0 brain_upserted=0 |  |  |

## Log
- `04:02:55` Firing crawler — this takes 1-5 min depending on note count…
## 2. Verify mirror

- `04:03:16` ⚠ Mirror is empty — TV may not expose a notes API on your plan; see next steps below
## 3. Invoke justhodl-brain-compiler

- `04:03:16` Skipping brain-compiler (no notes yet)
## 4. Status feed

- `04:03:17` ✗ crawler returned ok=False — check session validity
- `04:03:17` ✗ session_valid=False — session may be stale or wrong
