## Deploy global-sovereign v1.1.0 (core-DM CDS)

**Status:** success  
**Duration:** 194.3s  
**Finished:** 2026-07-16T15:40:25+00:00  

## Error

```
SystemExit: 0
```

## Log
- `15:37:11`   zip: 79792 bytes
## 1. Lambda

- `15:37:11`   Lambda exists — updating
- `15:37:17` ✅   ✓ updated justhodl-global-sovereign
## 2. EB rule + permissions

- `15:37:18`   rule already correct: global-sovereign-12h (cron(15 6,18 * * ? *))
- `15:37:18` ✅   ✓ target → justhodl-global-sovereign
- `15:37:18` ✅   ✓ added invoke permission
- `15:37:18` harvesting…
- `15:40:25` ✗ core_dm_cds not computed
