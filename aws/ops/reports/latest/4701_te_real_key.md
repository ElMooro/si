# ops 4701 — real paid TE key vs the historical credit-spread endpoint

**Status:** failure  
**Duration:** 3.7s  
**Finished:** 2026-08-15T16:17:00+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Confirm the real key resolves

- `16:16:57`   key resolved, len=31 (never printed)
## 2. Minimal validity check — one cheap call, exactly what te-feed already calls successfully

- `16:16:57`   status=200 rows=405 (matches te-feed's own working call shape)
- `16:16:57` ✅   key is LIVE and valid
## 3. THE REAL TEST — historical endpoint for a credit-spread indicator (te-feed never calls this path)

- `16:16:57`   [historical/indicator (spread)] status=200 rows=0 dates=None..None sample=[]
- `16:16:58`   [historical w/ explicit date range] status=200 rows=0 dates=None..None sample=[]
- `16:16:59`   [markets bond-spread search] status=200 rows=0 dates=None..None sample=[]
## verdict

- `16:17:00` ✗ real paid key works for country-snapshots (as te-feed already proves) but the historical credit-spread endpoint did not return deep dated data on this pass
