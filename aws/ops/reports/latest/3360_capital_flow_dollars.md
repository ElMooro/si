## G1 — FMP positions-summary probe (AAPL)

**Status:** success  
**Duration:** 19.6s  
**Finished:** 2026-07-16T23:38:34+00:00  

## Log
- `23:38:15` ✅ G1 ✓ dollars confirmed (AAPL $2.37T, 6,347 institutions)
## Deploy justhodl-capital-flow v2.0

- `23:38:15`   zip: 82594 bytes
## 1. Lambda

- `23:38:15`   Lambda exists — updating
- `23:38:21` ✅   ✓ updated justhodl-capital-flow
## 2. EB rule + permissions

- `23:38:21`   rule already correct: justhodl-capital-flow-daily (cron(30 16 * * ? *))
- `23:38:21` ✅   ✓ target → justhodl-capital-flow
- `23:38:21` ✅   ✓ added invoke permission
## G2 — deployed-code marker

- `23:38:28` ✅ G2 ✓ v2 code deployed & settled
## G3 — invoke + feed truth-bands

- `23:38:34` ✅ G3 ✓ v2 feed live — 84 $-enriched, net inst $ -27885161947, in/out 20/20
## G4 — history ledger

- `23:38:34` ✅ G4 ✓ ledger 1 entries, today tracks 95 flagged names
## G5 — live page

- `23:38:34` ✅ G5 ✓ Dollar Flows tab serving on justhodl.ai
- `23:38:34` ✅ VERDICT: PASS
