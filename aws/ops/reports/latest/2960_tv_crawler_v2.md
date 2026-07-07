- `04:10:12` ⚠ sessionid_sign NOT in SSM — TV 403s without it.
Run these two GitBash commands (already have sessionid):

1. In Chrome on tradingview.com -> Ctrl+Shift+I -> Application
   -> Cookies -> tradingview.com -> find 'sessionid_sign' row
   -> double-click the Value column, copy the whole string

2. GitBash command:
   MSYS_NO_PATHCONV=1 aws ssm put-parameter --name /justhodl/tradingview/sessionid_sign --type SecureString --value "PASTE_SESSIONID_SIGN_HERE" --overwrite --region us-east-1

The crawler will then work fully autonomously.
## Deploy crawler v2
**Status:** failure  
**Duration:** 32.6s  
**Finished:** 2026-07-07T04:10:44+00:00  

## Error

```
SystemExit: 1
```

## Data

| brain_upserted | elapsed | notes_harvested | session_in_ssm | session_len | session_valid | sign_in_ssm | sign_len | summary | symbols_covered | username |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | True | 33 |  | False | 0 |  |  |  |
| 0 | 26.6 | 0 |  |  | False |  |  |  | 0 | None |
|  |  |  |  |  |  |  |  | tv-crawler-v2: session=True sign=False |  |  |

## Log

- `04:10:12`   zip: 5780 bytes
## 1. Lambda

- `04:10:12`   Lambda exists — updating
- `04:10:17` ✅   ✓ updated justhodl-tv-notes-crawler
## 2. EB rule + permissions

- `04:10:17`   rule already correct: justhodl-tv-notes-crawler-daily (cron(0 6 * * ? *))
- `04:10:17` ✅   ✓ target → justhodl-tv-notes-crawler
- `04:10:17` ✅   ✓ added invoke permission
## Fire immediate harvest

- `04:10:44` ✗ session_valid=False even with sign — check cookie values
