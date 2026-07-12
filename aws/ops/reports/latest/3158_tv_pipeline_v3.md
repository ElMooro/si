# ops 3158 — TV pipeline v3 (notes + WATCHLISTS)

**Status:** success  
**Duration:** 8.6s  
**Finished:** 2026-07-12T18:46:41+00:00  

## Error

```
SystemExit: 0
```

## Data

| brain_upserted | e2e_status | function_url | mirror_added | n_fails | n_warns | verdict | watchlists_saved | zip_files | zip_kb |
|---|---|---|---|---|---|---|---|---|---|
|  |  | https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws/ |  |  |  |  |  |  |  |
| 1 | True |  | 1 |  |  |  | 2 |  |  |
|  |  |  |  |  |  |  |  | 8 | 13.0 |
|  |  |  |  | 0 | 1 | PASS |  |  |  |

## Log
## 1. Deploy ingest lambda

- `18:46:33`   zip: 55787 bytes
## 1. Lambda

- `18:46:33`   Lambda exists — updating
- `18:46:36` ✅   ✓ updated justhodl-tv-notes-ingest
## 2. E2E: notes + watchlists through the real pipe

- `18:46:37` ✅ health GET 200: {"ok": true, "service": "tv-notes-ingest", "mirror_count": 1}
- `18:46:39` ✅ tv-watchlists.json live: 2 lists, e2e lists present with full membership
- `18:46:41` e2e note deleted via delete_ids
- `18:46:41` e2e watchlists stripped — doc clean for the real sync
## 3. Package extension 1.1.0

- `18:46:41` ✅ zip → repo tools/ + S3 tools/jh-tv-extension.zip
## 4. Download path verification

- `18:46:41` ✅ tv-ingest-config.json: HTTP 200 (276 bytes)
- `18:46:41` ⚠ jh-tv-extension.zip: HTTP Error 403: Forbidden
