# ops 6658 - justhodl-bank-factors: declare, first join, data.html card, read back

**Status:** failure  
**Duration:** 1035.4s  
**Finished:** 2026-10-26T04:04:46+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. function + schedule

- `03:47:31` justhodl-bank-factors: runtime python3.12, memory 3008, timeout 900, state Active, modified 2026-10-26T03:46:17
- `03:47:31` schedule justhodl-bank-factors-daily: cron(50 2 * * ? *) ENABLED tz UTC target justhodl-bank-factors
## 2. deployed schedule manifest := committed (additions-only)

- `03:47:32` S3 1036 entries, repo 1037; added [('scheduler', 'justhodl-bank-factors-daily')] removed 0 changed 0
- `03:47:33` ✅ manifest uploaded (+1), backup backups/schedule-manifest/20261026T034732Z/
## 3. first join (async invoke, poll the state)

- `03:47:33` async invoke 202 at 03:47:33
- `04:04:46` ⚠ no finished state within 17 min - read CloudWatch /aws/lambda/justhodl-bank-factors
- `04:04:46` manifest: symbols None universe None common None etfs None otc None
- `04:04:46` as_of: null
- `04:04:46` coverage (symbols with the block): null
- `04:04:46` sources: null
- `04:04:46` schema_found: 0 fields resolved, unresolved: []
- `04:04:46` schema_found detail: {}
- `04:04:46` ✗ no factor book written
