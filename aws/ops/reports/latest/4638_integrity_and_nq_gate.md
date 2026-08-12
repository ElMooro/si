# ops 4638 — integrity + grammar + NQ door

**Status:** failure  
**Duration:** 510.0s  
**Finished:** 2026-08-12T17:52:05+00:00  

## Error

```
SystemExit: 1
```

## Data

| bytes | proxy | smoke |
|---|---|---|
| 142 | https://justhodl-nq-proxy.raafouis.workers.dev | PASS |

## Log
## direct code deploy (ops-side)

- `17:43:41` ✅   [code-deploy] justhodl-liquidity-reversal pushed from checkout
- `17:43:46` ✅   [code-deploy] justhodl-blackswan-watch pushed from checkout
## NQ egress worker (Cloudflare)

- `17:43:47` proxy reuse probe: HTTP Error 403: Forbidden
- `17:44:02` ✅   [nq-door] worker live + env injected on both engines
## deploy-settle both engines

- `17:52:05` ✗   [deploy] CONTRACT MISS — liq v1.3.0 (restored base) + blackswan v1.9.0
