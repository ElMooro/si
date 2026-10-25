# ops 6646 - justhodl-fmp-walker: declare, first run + 1 kick, data.html card, chain proof

**Status:** failure  
**Duration:** 965.8s  
**Finished:** 2026-10-25T16:42:59+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. function + schedule

- `16:26:54` justhodl-fmp-walker: runtime python3.12, memory 1536, timeout 900, state Active, modified 2026-10-25T16:16:29
- `16:26:54` env keys: ['FMP_KEY', 'FMP_WALKER_BUDGET_S', 'FMP_WALKER_RPS', 'FMP_WALKER_WORKERS', 'S3_BUCKET'] (values never printed)
- `16:26:54` schedule justhodl-fmp-walker-daily: cron(27 23 * * ? *) ENABLED tz UTC target justhodl-fmp-walker
## 2. deployed schedule manifest := committed (additions-only)

- `16:26:54` S3 1028 entries, repo 1029; added [('scheduler', 'justhodl-fmp-walker-daily')] removed 0 changed 0
- `16:26:54` ✅ manifest uploaded (+1), backup backups/schedule-manifest/20261025T162654Z/
## 3. first run + backfill kicks

- `16:26:54` kick 1: async invoke 202 at 16:26:54
- `16:42:59` ⚠ no state write within 16 min after kick 1 - read CloudWatch /aws/lambda/justhodl-fmp-walker
- `16:42:59` endpoints served: None
- `16:42:59` endpoints refused (recorded, not guessed): []
- `16:42:59` universes: None
- `16:42:59` per-dataset (first 40): {}
- `16:42:59` ✗ nothing banked after 1 kicks - see catalog verdicts above
