# ops 3735 — SHIP justhodl-grid-queue v1.0 (power buildout canary)

**Status:** failure  
**Duration:** 1.0s  
**Finished:** 2026-07-22T21:14:37+00:00  

## Error

```
SystemExit: 1
```

## Data

| data_month | failed | function | n_lines | signals | verdict |
|---|---|---|---|---|---|
| ? | G3_invoke_accepted,G4_artifact,G5_queue_parsed,G6_eia_legs,G7_schedule | justhodl-grid-queue | 0 | 0 | FAIL |

## Log
## G0 — key contract (grep producer before gating)

- `21:14:36` PASS G0_key_contract — engine emits all gated keys
## G1 — zip settle (never invoke the old artifact)

- `21:14:37` PASS G1_zip_settle — marker found after 0s
## G2 — EIA_API_KEY inherited

- `21:14:37` PASS G2_eia_key — key present len=40
## G3 — async invoke (Event) + S3 freshness gate

- `21:14:37`   no prior artifact (first run)
- `21:14:37` FAIL G3_invoke_accepted — ResourceConflictException An error occurred (ResourceConflictException) when calling the Invoke operation: The operation cannot be performed at this time. The function is currently in the following state: Pending
## G4 — S3 artifact fresh + shape

- `21:14:37` FAIL G4_artifact — ClientError An error occurred (404) when calling the HeadObject operation: Not Found
## G5 — data truth (CAISO parse is real, not empty)

- `21:14:37` FAIL G5_queue_parsed — no doc
## G6 — EIA planned capacity + industrial load + hotspots

- `21:14:37` FAIL G6_eia_legs — no doc
## G7 — schedule

- `21:14:37` FAIL G7_schedule — NO SCHEDULE
## VERDICT

- `21:14:37` ✗ gates failed: ['G3_invoke_accepted', 'G4_artifact', 'G5_queue_parsed', 'G6_eia_legs', 'G7_schedule']
