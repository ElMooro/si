## SSM check

**Status:** failure  
**Duration:** 38.6s  
**Finished:** 2026-07-07T04:21:45+00:00  

## Error

```
SystemExit: 1
```

## Data

| brain_errors | brain_upserted | device_t_len | elapsed_s | ingest_url | mirror_count | mirror_updated | notes_harvested | notes_in_mirror | session_valid | sessionid_len | sessionid_sign_len | summary | symbols_covered | token_ok | username |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | 52 |  |  |  |  |  |  |  | 33 | 47 |  |  | True |  |
|  |  |  |  | https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws |  |  |  |  |  |  |  |  |  |  |  |
| 0 | 0 |  | 26.7 |  |  |  | 0 | 0 | False |  |  |  | 0 |  | None |
|  |  |  |  |  | 0 | 2026-07-07T04:21:44.807756+00:00 |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | tv-harvest: session_valid=False notes=0 tickers=0 mirror=0 brain_upserted=0 |  |  |  |

## Log
## Redeploy crawler with fresh env

- `04:21:07`   zip: 5780 bytes
## 1. Lambda

- `04:21:08`   Lambda exists — updating
- `04:21:13` ✅   ✓ updated justhodl-tv-notes-crawler
## 2. EB rule + permissions

- `04:21:13`   rule already correct: justhodl-tv-notes-crawler-daily (cron(0 6 * * ? *))
- `04:21:14` ✅   ✓ target → justhodl-tv-notes-crawler
- `04:21:14` ✅   ✓ added invoke permission
## Invoke crawler

- `04:21:18` Invoking justhodl-tv-notes-crawler synchronously (up to 9 min)...
## Mirror verification

- `04:21:45` No notes in mirror — skipping brain-compiler
- `04:21:45` ✗ session_valid=False after 27s. TV may need a different cookie. Check:
  1. In Chrome DevTools Application->Cookies, try copying all cookies as a full Cookie header string and storing as /justhodl/tradingview/full_cookie_header
  2. Make sure you are logged in on tradingview.COM (not .TV)
  3. Try re-logging in and re-copying the cookie
