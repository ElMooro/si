# ops 3257 — notes restored: guard → brain-restore → live crawl → intel rebuild

**Status:** success  
**Duration:** 117.5s  
**Finished:** 2026-07-13T13:38:57+00:00  

## Data

| brain_notes | brain_tv_notes | mirror_after_crawl | mirror_before | mirror_final | mirror_source | n_fails | n_warns | verdict |
|---|---|---|---|---|---|---|---|---|
| 9200 | 0 |  | 3322 |  |  |  |  |  |
|  |  | 3322 |  |  | justhodl-tv-notes-crawler |  |  |  |
|  |  |  |  | 3322 |  | 0 | 1 | PASS |

## Log
## 1. Guarded crawler deployed first

- `13:36:59`   zip: 78466 bytes
## 1. Lambda

- `13:37:00`   Lambda exists — updating
- `13:37:05` ✅   ✓ updated justhodl-tv-notes-crawler
- `13:37:05` ✅   ✓ Function URL: https://nkw3cbwfhkfbd7febm4eze6cua0wzgya.lambda-url.us-east-1.on.aws/
## 2. Restore from the brain

## 3. Live crawl (cookie state = cadence)

- `13:38:48`   [crawler] starting at 2026-07-13T06:00:18.979193+00:00
- `13:38:48`   [crawler] session_len=32 sign_len=47 device_t_len=52
- `13:38:48`   [crawler] username: None
- `13:38:48`   [crawler] after bulk pull: 0 notes
- `13:38:48`   [crawler] per-symbol sweep: 0 symbols
- `13:38:48`   [crawler] after layout scan: 0 notes total
- `13:38:48`   [crawler] mirror written: 3322 notes
- `13:38:48`   [crawler] done in 26.1s
## 4. notes-intel rebuild

- `13:38:56` ✅ notes-index rebuilt: 536 tickers, n_notes=3322
- `13:38:56` ⚠ brain carries no tv-provenance notes at data/brain.json — restore skipped
