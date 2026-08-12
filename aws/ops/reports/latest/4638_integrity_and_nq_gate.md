# ops 4638 — integrity + grammar + NQ door

**Status:** failure  
**Duration:** 498.6s  
**Finished:** 2026-08-12T16:05:14+00:00  

## Error

```
SystemExit: 1
```

## Data

| bytes | proxy | smoke |
|---|---|---|
| 142 | https://justhodl-nq-proxy.raafouis.workers.dev | PASS |

## Log
## NQ egress worker (Cloudflare)

- `15:57:10` ✅   [nq-door] worker live + env injected on both engines
## deploy-settle both engines

- `16:05:14` ✗   [deploy] CONTRACT MISS — liq v1.3.0 (restored base) + blackswan v1.9.0
