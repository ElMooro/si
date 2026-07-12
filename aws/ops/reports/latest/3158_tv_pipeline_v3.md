# ops 3158 — TV pipeline v3 (notes + WATCHLISTS)

**Status:** failure  
**Duration:** 18.0s  
**Finished:** 2026-07-12T18:41:46+00:00  

## Error

```
SystemExit: 1
```

## Data

| brain_upserted | e2e_status | function_url | mirror_added | n_fails | n_warns | verdict | watchlists_saved | zip_files | zip_kb |
|---|---|---|---|---|---|---|---|---|---|
|  |  | https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws/ |  |  |  |  |  |  |  |
| None | None |  | None |  |  |  | None |  |  |
|  |  |  |  |  |  |  |  | 8 | 13.0 |
|  |  |  |  | 2 | 1 | FAIL |  |  |  |

## Log
## 1. Deploy ingest lambda

- `18:41:29`   zip: 55246 bytes
## 1. Lambda

- `18:41:29`   Lambda exists — updating
- `18:41:34` ✅   ✓ updated justhodl-tv-notes-ingest
## 2. E2E: notes + watchlists through the real pipe

- `18:41:36` ✅ health GET 200: {"ok": true, "service": "tv-notes-ingest", "mirror_count": 1}
- `18:41:43` CW: [ERROR] NameError: name 'wl_saved' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 214, in lambda_handler
    "watchlists_saved": wl_saved,
- `18:41:46` e2e note deleted via delete_ids
- `18:41:46` e2e watchlists stripped — doc clean for the real sync
## 3. Package extension 1.1.0

- `18:41:46` ✅ zip → repo tools/ + S3 tools/jh-tv-extension.zip
## 4. Download path verification

- `18:41:46` ✅ tv-ingest-config.json: HTTP 200 (276 bytes)
- `18:41:46` ⚠ jh-tv-extension.zip: HTTP Error 403: Forbidden
- `18:41:46` ✗ E2E POST failed: HTTP Error 502: Bad Gateway — CW above
- `18:41:46` ✗ watchlists_saved=None (expected 2)
