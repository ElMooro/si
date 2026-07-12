# ops 3140 — Telegram token probe + wire

**Status:** failure  
**Duration:** 5.5s  
**Finished:** 2026-07-12T03:46:45+00:00  

## Error

```
SystemExit: 1
```

## Data

| n_fails | n_warns | verdict |
|---|---|---|
| 1 | 0 | FAIL |

## Log
## 1. Probe candidates (runner-side getMe)

- `03:46:40` donor-config: LIVE @Justhodl_bot
- `03:46:40` runner-secret: dead (empty)
## 2. Merge live token into function env

- `03:46:44` ✅ env updated with donor-config token (keys: ['FMP_API_KEY', 'TELEGRAM_CHAT_ID', 'TELEGRAM_TOKEN'])
## 3. Armed test via lambda

- `03:46:45` ✗ lambda send still failing with a proven-live token — inspect send path next op
