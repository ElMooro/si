# ops 4606 — Cloudflare token restored: purge probe

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-08-11T21:14:56+00:00  

## Data

| purge_err | purge_msgs | purge_success | token_status | verify_err | verify_msgs | verify_success |
|---|---|---|---|---|---|---|
|  |  |  | active | None | [] | True |
| None | [] | True |  |  |  |  |

## Log
## token verify

- `21:14:55` ✅   [env] CLOUDFLARE_API_TOKEN present in runner env
- `21:14:55` ✅   [verify] token verifies active (status=active err=None)
## zone + purge

- `21:14:55` ✅   [zone] zone justhodl.ai resolved (id=fb59e2d09c34b648d334696ae5c02a0a err=None)
- `21:14:56` ✅   [purge] cache purge accepted for the plumbing surfaces
## edge sanity

- `21:14:56` ✅   [edge] post-purge edge serves plumbing.html with the L0 card
## verdict

- `21:14:56` ✅ Cloudflare purge path RESTORED — token active, zone resolved, purge accepted, edge fresh. Every future ops purge and pages.yml post-deploy purge now works again. Note: this token was once pasted in chat (April); when convenient, roll it and hand me the new value — I will re-seal the secret the same way.
