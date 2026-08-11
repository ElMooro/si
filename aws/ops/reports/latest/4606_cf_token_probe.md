# ops 4606 — Cloudflare token restored: purge probe

**Status:** failure  
**Duration:** 0.8s  
**Finished:** 2026-08-11T21:08:24+00:00  

## Error

```
SystemExit: 1
```

## Data

| purge_err | purge_msgs | purge_success | token_status | verify_err | verify_msgs | verify_success |
|---|---|---|---|---|---|---|
|  |  |  | active | None | [] | True |
| HTTP 401 | [{"code": 10000, "message": "Authentication error"}] | False |  |  |  |  |

## Log
## token verify

- `21:08:23` ✅   [env] CLOUDFLARE_API_TOKEN present in runner env
- `21:08:23` ✅   [verify] token verifies active (status=active err=None)
## zone + purge

- `21:08:24` ✅   [zone] zone justhodl.ai resolved (id=fb59e2d09c34b648d334696ae5c02a0a err=None)
- `21:08:24` ✗   [purge] CONTRACT MISS — cache purge accepted for the plumbing surfaces
## edge sanity

- `21:08:24` ✅   [edge] post-purge edge serves plumbing.html with the L0 card
## verdict

- `21:08:24` ✗ cf probe: 1 red
