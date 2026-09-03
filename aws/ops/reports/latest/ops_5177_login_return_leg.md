# ops 5177 -- Google sign-in return leg (PKCE-correct)

**Status:** failure  
**Duration:** 33.8s  
**Finished:** 2026-09-03T22:59:24+00:00  

## Error

```
SystemExit: 1
```

## Log
- `22:59:09`    /my-portfolio.html             went to Google=True  token calls=[]  sb keys=[]
- `22:59:24`    /chart-pro.html?s=VLO&tf=1W    went to Google=True  token calls=[]  sb keys=[]
- `22:59:24` ✗    /my-portfolio.html: no /auth/v1/token exchange on return with a stored verifier
- `22:59:24` ✗    /chart-pro.html?s=VLO&tf=1W: no /auth/v1/token exchange on return with a stored verifier
