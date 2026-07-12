# ops 3162 — authoritative ingest config

**Status:** success  
**Duration:** 3.3s  
**Finished:** 2026-07-12T20:37:04+00:00  

## Error

```
SystemExit: 0
```

## Data

| config_url | live_url | match | n_fails | n_warns | probe_status | verdict | watchlists_saved |
|---|---|---|---|---|---|---|---|
| https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws | https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws/ | True |  |  |  |  |  |
|  |  |  |  |  | 200 |  | 1 |
|  |  |  | 0 | 0 |  | PASS |  |

## Log
## 1. Live URL vs published config

## 2. Republish config

- `20:37:02` ✅ config republished with the live URL + token
## 3. Public fetch (the path the extension takes)

- `20:37:02` ✅ justhodl-dashboard-live.s3.u… HTTP 200 · url matches live: True
- `20:37:03` ✅ justhodl.ai… HTTP 200 · url matches live: True
## 4. Watchlists-only probe through the config URL

- `20:37:03` ✅ END-TO-END: config URL accepts watchlists — the extension's exact path is now clear
- `20:37:04` probe watchlist stripped
