# ops 3730 — SHIP justhodl-import-canary v1.0 (import flow canary)

**Status:** failure  
**Duration:** 272.0s  
**Finished:** 2026-07-22T20:33:16+00:00  

## Error

```
SystemExit: 1
```

## Data

| data_month | failed | function | n_lines | signals | verdict |
|---|---|---|---|---|---|
| ? | G3_invoke,G4_artifact,G5_data_truth,G6_signals | justhodl-import-canary | 0 | 0 | FAIL |

## Log
## G0 — key contract (grep producer before gating)

- `20:28:44` PASS G0_key_contract — engine emits all gated keys
## G1 — zip settle (never invoke the old artifact)

- `20:28:44` PASS G1_zip_settle — marker found after 0s
## G2 — CENSUS_API_KEY inherited

- `20:28:44` PASS G2_census_key — key present len=40
## G3 — invoke

- `20:33:15` FAIL G3_invoke — ConnectionClosedError Connection was closed before we received a valid response from endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-import-canary/invocations".
## G4 — S3 artifact fresh + shape

- `20:33:15` FAIL G4_artifact — ClientError An error occurred (404) when calling the HeadObject operation: Not Found
## G5 — data truth (real numbers, no fabrication)

- `20:33:15` FAIL G5_data_truth — no doc
## G6 — signal ladder + industry rollup

- `20:33:15` FAIL G6_signals — no doc
## G7 — schedule

- `20:33:16`   eventbridge rule: justhodl-import-canary-daily cron(40 13 * * ? *)
- `20:33:16` PASS G7_schedule — schedule present
## VERDICT

- `20:33:16` ✗ gates failed: ['G3_invoke', 'G4_artifact', 'G5_data_truth', 'G6_signals']
