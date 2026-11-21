# ops 6887 - the fleet's bundled history-book reader (read-only), 2026-11-21T19:16+00:00

**Status:** success  
**Duration:** 13.8s  
**Finished:** 2026-11-21T19:17:09+00:00  

## Data

| carrying_new_reader | held_back | importers | not_ready |
|---|---|---|---|
| 224 | ['justhodl-bond-desk'] | 225 | 0 |

## Log
- `19:16:55` 225 engines import aws/shared/fmp_bank.py; repository copy sha256 d63df0628563d616 (42559 bytes); new-layout marker present: True
## 1. Every importer

- `19:17:09` 225 packages read (283 MB): {"held back: OLD READER": 1, "ok": 224}
- `19:17:09` LastModified of the importers: earliest 2026-10-26T21:33:19, latest 2026-11-21T19:12:28; updated since the lane started (2026-11-21T17:37:00): 224
- `19:17:09`   justhodl-bond-desk                           held back: OLD READER | modified 2026-10-26T21:33:19
- `19:17:09` ✅ every importer except the one held back carries the reader that understands both layouts
