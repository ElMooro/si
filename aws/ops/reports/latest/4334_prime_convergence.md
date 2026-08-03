# ops 4334 -- the fingerprint becomes a tier

**Status:** failure  
**Duration:** 5.0s  
**Finished:** 2026-08-03T20:53:21+00:00  

## Error

```
SystemExit: 1
```

## Log
- `20:53:21` root: {"statusCode": 200, "body": "{\"n_compound\": 133, \"n_3_plus\": 41, \"n_alerts\": 30, \"duration_s\": 1.6}"}
- `20:53:21` AAPL: prime=None combo= archetype=None rc=None pct_all=None
- `20:53:21` GOOGL: prime=None combo= archetype=None rc=None pct_all=None
- `20:53:21` MSFT: prime=None combo= archetype=None rc=None pct_all=None
- `20:53:21` ✅ ORCL correctly not prime
- `20:53:21` ✅ prime artifact: n=0 rows=[]
- `20:53:21` history days: 1
- `20:53:21` ✗   AAPL not prime (systems drifted since flag day? combo=None)
- `20:53:21` ✗   GOOGL not prime (systems drifted since flag day? combo=None)
- `20:53:21` ✗   MSFT not prime (systems drifted since flag day? combo=None)
- `20:53:21` ✗   prime/history artifacts thin
