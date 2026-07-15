## Deploy sovereign-stress CISS fallback fix

**Status:** success  
**Duration:** 39.9s  
**Finished:** 2026-07-15T20:53:41+00:00  

## Log
- `20:53:01`   zip: 81846 bytes
## 1. Lambda

- `20:53:02`   Lambda exists — updating
- `20:53:09` ✅   ✓ updated justhodl-sovereign-stress
## 2. EB rule + permissions

- `20:53:09`   rule already correct: sovereign-stress-daily (cron(0 12 ? * * *))
- `20:53:09` ✅   ✓ target → justhodl-sovereign-stress
- `20:53:09` ✅   ✓ added invoke permission
- `20:53:38`   return: {"statusCode": 200, "body": "{\"ok\": true, \"regime\": \"NORMAL\", \"europe_score\": 30.7, \"errors\": 0}"}
- `20:53:41`   errors: 0 → []
- `20:53:41`   CISS regions: ['euro_area', 'united_states', 'china', 'united_kingdom']
- `20:53:41`   europe score=30.7 regime=NORMAL
- `20:53:41` ✅ SOVEREIGN-STRESS CLEAN — 0 errors, all 4 CISS regions populate.
- `20:53:41` ✅ all 4 CISS regions (EA/US/CN/UK) now live.
