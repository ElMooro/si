# ops 3810 — v5.0 mispricing verdict (why/who/what)

**Status:** failure  
**Duration:** 35.3s  
**Finished:** 2026-07-24T17:43:26+00:00  

## Error

```
SystemExit: 1
```

## Data

| invoke_seconds | invoke_status |
|---|---|
| 16.1 | 200 |

## Log
## G0 — keys verified live in ops 3809

- `17:42:51` ✅ G0.v441 :: engine at v4.4.1
- `17:42:51` ✅ G0.struct :: structural score present
- `17:42:51` ✅ G0.ledger_var :: ledger rows in scope for persistence
- `17:42:51` ✅ G0.datetime :: datetime available
- `17:42:51` ✅ G0.anchor :: 12-space anchor after structural try/except (count=1)
- `17:42:51` ✅ G0.direction_map :: data/estimate-revisions.json -> direction_map n=393
- `17:42:51` ✅ G0.dark_map :: data/dark-pool.json -> dark_map n=939
- `17:42:51` ✅ G0.tickers :: data/finra-short.json -> tickers n=501
- `17:42:51` ✅ G0.all_qualifying :: data/earnings-pead.json -> all_qualifying n=244
- `17:42:51` ✅ G0.league :: data/industry-boom.json -> league n=119
## Splice v5.0

- `17:42:51` ✅ v5.0 spliced + compile clean
## Deploy

- `17:42:52`   zip: 107864 bytes
## 1. Lambda

- `17:42:52`   Lambda exists — updating
- `17:42:55` ✅   ✓ updated justhodl-chokepoint
- `17:43:10` ✅ settled attempt 1
- `17:43:10` ✅ DEPLOY.settled :: v5.0 live
## Invoke

- `17:43:26` ✅ LIVE.v50 :: version=5.0
- `17:43:26` ✗ LIVE.no_err :: err='int' object has no attribute 'get'
## Join coverage — every leg must be alive

## Verdict distribution

- `17:43:26` ✗ VERDICT.discriminates :: more than one verdict class populated
- `17:43:26` ✅ VERDICT.not_all_mispriced :: MISPRICED=None is a minority, not a rubber stamp
## MISPRICED book — gap + importance + 2 confirmations, no disqualifier

## VALUE_TRAP book — the names this engine used to rank highly

## Additive

- `17:43:26` ✅ ADDITIVE.capture_gap :: preserved
- `17:43:26` ✅ ADDITIVE.structural_importance :: preserved
- `17:43:26` ✅ ADDITIVE.catchup_pct :: preserved
- `17:43:26` ✅ ADDITIVE.revenue_share_pct :: preserved
## VERDICT

- `17:43:26` ✗ FAILED: LIVE.no_err, VERDICT.discriminates
