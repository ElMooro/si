## P0 persist credential

**Status:** failure  
**Duration:** 504.4s  
**Finished:** 2026-08-25T16:44:57+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:36:33`   vault item written (hash key 'key_hash') secret_present=False
- `16:36:33`   engine env: CLIENT_ID set, secret=False, expiry recorded
## P1 token mint

- `16:36:53`   SECRET MISSING: OAuth2 client-credentials needs the API Client Secret paired with b288c3b3... -- paste it (or add client_secret to the vault's finra item) and the engine upgrades on the next 6h tick. Public-tier drain continues meanwhile.
## P2 drain truth + expiry note

- `16:37:24`   t+ 30s phase=DRAIN banked=0 rows=0 q=0
- `16:37:54`   t+ 60s phase=DRAIN banked=0 rows=0 q=0
- `16:38:24`   t+ 90s phase=DRAIN banked=0 rows=0 q=0
- `16:38:54`   t+120s phase=DRAIN banked=0 rows=0 q=0
- `16:39:25`   t+150s phase=DRAIN banked=0 rows=0 q=0
- `16:39:55`   t+181s phase=DRAIN banked=0 rows=0 q=0
- `16:40:25`   t+211s phase=DRAIN banked=0 rows=0 q=0
- `16:40:55`   t+241s phase=DRAIN banked=0 rows=0 q=0
- `16:41:25`   t+271s phase=DRAIN banked=0 rows=0 q=0
- `16:41:55`   t+301s phase=DRAIN banked=0 rows=0 q=0
- `16:42:26`   t+332s phase=DRAIN banked=0 rows=0 q=0
- `16:42:56`   t+362s phase=DRAIN banked=0 rows=0 q=0
- `16:43:26`   t+392s phase=DRAIN banked=0 rows=0 q=0
- `16:43:56`   t+422s phase=DRAIN banked=0 rows=0 q=0
- `16:44:27`   t+452s phase=DRAIN banked=0 rows=0 q=0
- `16:44:57`   t+483s phase=DRAIN banked=0 rows=0 q=0
- `16:44:57`   P2 FAIL (banked 0 -> 0, kicked=True)
- `16:44:57`   manifest expiry note written
- `16:44:57` ops 4979 RED: P2
