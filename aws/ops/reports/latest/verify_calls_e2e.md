# 1) calls.html on production

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-05-04T22:47:11+00:00  

## Log
- `22:47:10`   ✓ status=200, size=23,222b
- `22:47:10`     ✓ title
- `22:47:10`     ✓ nav active
- `22:47:10`     ✓ now banner
- `22:47:10`     ✓ KPI row
- `22:47:10`     ✓ timeline svg
- `22:47:10`     ✓ changes section
- `22:47:10`     ✓ history table
- `22:47:10`     ✓ auto-refresh
- `22:47:10`     ✓ loads ledger
- `22:47:10`     ✓ verb colors
# 2) Decisive-call ledger state

- `22:47:10`   n_snapshots: 3
- `22:47:10`   last_updated: 2026-05-04T22:32:18.443232+00:00
- `22:47:10` 
- `22:47:10`   All snapshots:
- `22:47:10`     ts=2026-05-04T22:23:15  call=UNKNOWN               highest=carry_risk  acc=0.5527
- `22:47:10`     ts=2026-05-04T22:28:14  call=UNKNOWN               highest=carry_risk  acc=0.5527
- `22:47:10`     ts=2026-05-04T22:32:18  call=EXIT_ALL_RISK         highest=carry_risk  acc=0.5527
# 3) Calls tab visible on key pages

- `22:47:10`   ✓ today.html                 Calls link: True
- `22:47:11`   ✓ brief.html                 Calls link: True
- `22:47:11`   ✓ performance.html           Calls link: True
- `22:47:11`   ✓ weights.html               Calls link: True
- `22:47:11`   ✓ accuracy.html              Calls link: True
# 4) position-monitor schedule + recent metrics

- `22:47:11`   state: Active, mem=256MB, timeout=60s
- `22:47:11`   last modified: 2026-05-04T22:44:07.409+0000
- `22:47:11`   ✓ schedule: rate(30 minutes) state=ENABLED
# 5) position-monitor state

- `22:47:11`   last_run: 2026-05-04T22:44:59.215955+00:00
- `22:47:11`   last_call_verb_seen: EXIT_ALL_RISK
- `22:47:11`   alerts_in_dedup_window: 0
