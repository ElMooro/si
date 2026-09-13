# ops 5526 -- evidence envelope on wall entries: receipt, live bytes, owner sandbox probe, boundaries

**Status:** success  
**Duration:** 3.6s  
**Finished:** 2026-09-13T20:02:31+00:00  

## Data

| head |
|---|
| 6de40ca1f1 |

## Log
- `20:02:28` ✅ justhodl-ai receipt commit=099f465 source_identical_to_HEAD=True sha_match=True run=34779186512
- `20:02:28` ✅ live zip: envelope code=True provider bypass absent=True
- `20:02:28` ✅ missing service identity -> 401 (want 401)
- `20:02:29` ✅ owner GET /factory/sandbox -> 200 contract=factory-evidence.v1 holdout_hash=None leak=False
- `20:02:31` ✅ anonymous factory/salon/season.json -> 403
- `20:02:31` ✅ GREEN -- evidence envelope live: contract + manifest digest on the sandbox, envelope code in the live zip, identity still server-side
