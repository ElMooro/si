# ops 5173 -- Google sign-in forensics (read-only)

**Status:** failure  
**Duration:** 50.3s  
**Finished:** 2026-09-03T22:55:12+00:00  

## Error

```
SystemExit: 1
```

## Data

| client_id | csp_ok | disable_signup | email | first_status | google | health | http | nav | page | redirect_uri | scripts | section | signin_btn | supabase_loaded | token_calls |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | False | True |  | True | 200 |  |  |  |  |  | S1 |  |  |  |
| 110542026797-794iqf9giskgjoodreg542g44oh5ogcl.apps.googleusercontent.com |  |  |  |  |  |  |  |  |  | https://bdmjenqcyvzouusfcgow.supabase.co/auth/v1/callback |  | S2 |  |  |  |
|  | True |  |  |  |  |  | 200 |  | /my-portfolio.html |  | 3 | S3 |  |  |  |
|  | True |  |  |  |  |  | 200 |  | /chart-pro.html |  | 3 | S3 |  |  |  |
|  |  |  |  |  |  |  |  | https://accounts.google.com/v3/signin/identifier?opparams=%253Fredirect_to%253Dh | /my-portfolio.html |  |  | S4_click | True | True |  |
|  |  |  |  | None |  |  |  |  | /my-portfolio.html |  |  | S4_return |  |  | 0 |
|  |  |  |  |  |  |  |  | https://accounts.google.com/v3/signin/identifier?opparams=%253Fredirect_to%253Dh | /chart-pro.html?s=VLO&tf=1W |  |  | S4_click | True | True |  |
|  |  |  |  | None |  |  |  |  | /chart-pro.html?s=VLO&tf=1W |  |  | S4_return |  |  | 0 |

## Log
## P0 wait for the restored project (DNS + /auth/v1/health), up to 14 minutes

- `22:54:22` ✅    bdmjenqcyvzouusfcgow.supabase.co resolves and /auth/v1/health -> 200 after 0s
## S1 Supabase project health + auth settings

- `22:54:22`    GET /auth/v1/health -> 200 {"version":"v2.196.0","name":"GoTrue","description":"GoTrue is a user registration and authentication API"}
- `22:54:22`    GET /auth/v1/settings -> 200  google=True email=True facebook=False  disable_signup=False  mailer_autoconfirm=False
- `22:54:22`    GET /rest/v1/profiles (anon) -> 200 []
## S2 OAuth start: Supabase authorize -> Google

- `22:54:23`    GET /auth/v1/authorize?provider=google -> 302  Location: https://accounts.google.com/o/oauth2/v2/auth?client_id=110542026797-794iqf9giskgjoodreg542g44oh5ogcl.apps.googleusercontent.com&redirect_to=https%3A%2F%2Fjustho
- `22:54:23`    client_id=110542026797-794iqf9giskgjoodreg542g44oh5ogcl.apps.googleusercontent.com  redirect_uri=https://bdmjenqcyvzouusfcgow.supabase.co/auth/v1/callback  scope=email profile
- `22:54:23`    GET Google authorize -> 200  title/text: window.WIZ_global_data = {"AfY8Hf":true,"DndLYb":"","DpimGf":false,"EP1ykd":["/_/*"],"EaOSAe":"https://kidsmanagement-pa.googleapis.com","FdrFJe":"-565150962003775305","FoW6je":false,"GWsdKe":"en-US","HAZvpc":"https://ac
- `22:54:23` ✅    Google serves the account chooser for this client -- client and redirect URI are valid
## S3 Site wiring at the edge

- `22:54:23`    /auth-config.js -> 200 enabled=true url=https://bdmjenqcyvzouusfcgow.supabase.co key=sb_publishable_W6V...
- `22:54:23`    supabase-js CDN -> 200  213176 bytes  version=2.114.0
- `22:54:24`    /my-portfolio.html   -> 200  scripts=['/auth-config.js?v=d35cea03', 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2', '/auth.js?v=00f49db2']  CSP present=True supabase+jsdelivr allowed=True
- `22:54:24` ⚠    script order on /my-portfolio.html is ['/auth-config.js?v=d35cea03', 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2', '/auth.js?v=00f49db2']
- `22:54:24`    /chart-pro.html      -> 200  scripts=['/auth-config.js?v=d35cea03', 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2', '/auth.js?v=00f49db2']  CSP present=True supabase+jsdelivr allowed=True
- `22:54:24` ⚠    script order on /chart-pro.html is ['/auth-config.js?v=d35cea03', 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2', '/auth.js?v=00f49db2']
## S4 Headless Chrome: click-through and return leg

- `22:54:41`    /my-portfolio.html               facts={"supabaseLoaded": true, "authObj": true, "signinBtn": true, "user": "no"} console=3 csp_violations=0
- `22:54:41`       console: error: Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fmy-portfolio.html&favs=0&sw=none&v=3276' from origin 'h
- `22:54:41`       console: error: Failed to load resource: net::ERR_FAILED
- `22:54:41`       console: error: Failed to load resource: the server responded with a status of 404 ()
- `22:54:42`    modal opened=True google button=True
- `22:54:46`    after click -> https://accounts.google.com/v3/signin/identifier?opparams=%253Fredirect_to%253Dhttps%25253A%25252F%25252Fjusthodl.ai%25252Fmy-portfolio.html&dsh=S641349052%3A1788476082901151&client_id=110542026797-79
- `22:54:46` ✅    reached Google's sign-in page (Sign in - Google Accounts)
- `22:54:53`    return leg /my-portfolio.html -> token exchange calls: [] ; url now https://justhodl.ai/my-portfolio.html?code=ops5171-fake-code
- `22:54:58`    /chart-pro.html?s=VLO&tf=1W      facts={"supabaseLoaded": true, "authObj": true, "signinBtn": true, "user": "no"} console=5 csp_violations=0
- `22:54:58`       console: error: Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fchart-pro.html&favs=0&sw=none&v=3276' from origin 'http
- `22:54:58`       console: error: Failed to load resource: net::ERR_FAILED
- `22:54:58`       console: error: Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=chartpro&lists=492&err=&sw=none&drawer=c3efc5d3&favs=0&lwc
- `22:54:58`       console: error: Failed to load resource: net::ERR_FAILED
- `22:54:58`       console: log: [JHF] fusion loaded: 683 phased, 1500 whale
- `22:54:59`    modal opened=True google button=True
- `22:55:03`    after click -> https://accounts.google.com/v3/signin/identifier?opparams=%253Fredirect_to%253Dhttps%25253A%25252F%25252Fjusthodl.ai%25252Fchart-pro.html%25253Fs%25253DVLO%252526tf%25253D1W%252526cmp%25253D&dsh=S-938
- `22:55:03` ✅    reached Google's sign-in page (Sign in - Google Accounts)
- `22:55:10`    return leg /chart-pro.html?s=VLO&tf=1W -> token exchange calls: [] ; url now https://justhodl.ai/chart-pro.html?s=VLO&tf=1W&code=ops5171-fake-code&cmp=
## P2 keep-alive sentinel: justhodl-supabase-keepalive every 4 hours + first run

- `22:55:12`    first run -> {"version": "1.0.0", "checked_at": "2026-09-03T22:55:12+00:00", "up": true, "dns": "ok", "host": "bdmjenqcyvzouusfcgow.supabase.co", "auth_settings": {"status": 200, "ms": 401, "google_enabled": null}, "rest_profiles": {"status": 200, "ms": 179}, "last_up": "2026-09-03T22:55:12+00:00", "state_change
- `22:55:12` ✅    schedule justhodl-supabase-keepalive rate(4 hours) armed
## S5 verdict

- `22:55:12` ✗    /my-portfolio.html: no /auth/v1/token exchange attempted on return (code stripped or supabase-js absent)
- `22:55:12` ✗    /chart-pro.html?s=VLO&tf=1W: no /auth/v1/token exchange attempted on return (code stripped or supabase-js absent)
- `22:55:12`    RED: 2 failing leg(s) listed above
