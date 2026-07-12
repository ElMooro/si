# ops 3142 — self-healing Telegram send path

**Status:** success  
**Duration:** 23.1s  
**Finished:** 2026-07-12T03:51:42+00:00  

## Error

```
SystemExit: 0
```

## Data

| delivered | n_fails | n_warns | verdict |
|---|---|---|---|
| False | 0 | 1 | PASS |

## Log
## 1. Deploy v2.1.1

- `03:51:19`   zip: 63917 bytes
## 1. Lambda

- `03:51:20`   Lambda exists — updating
- `03:51:26` ✅   ✓ updated justhodl-alpha-compass
## 2. EB rule + permissions

- `03:51:27`   rule already correct: alpha-compass-3h (cron(50 */3 * * ? *))
- `03:51:27` ✅   ✓ target → justhodl-alpha-compass
- `03:51:27` ✅   ✓ added invoke permission
## 3. Smoke test

- `03:51:27`   invoking justhodl-alpha-compass…
- `03:51:29` ✅   ✓ smoke test passed
- `03:51:29`     ok                       True
- `03:51:29`     cards                    7
- `03:51:29`     regime                   Normal
## 2. Output still healthy

- `03:51:29` ✅ fresh · 30d n=24 · 90d n=86 · regime=Normal
## 3. Heal-path exercise + CW detail

- `03:51:42` CW: [compass] could not load data/_telegram-chat.json: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `03:51:42` CW: [compass] telegram failed: {"ok":false,"error_code":403,"description":"Forbidden: bot can't initiate conversation with a user"}
- `03:51:42` ⚠ AWAITING ONE TAP: open t.me/Justhodl_bot and press Start — the next scheduled run (every 3h at :50) self-arms and persists the chat for the whole fleet to reuse
