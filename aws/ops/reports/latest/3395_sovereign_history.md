## Deploy global-sovereign v1.4.0 (history snapshotting)

**Status:** success  
**Duration:** 81.6s  
**Finished:** 2026-07-16T16:34:02+00:00  

## Log
- `16:32:41`   zip: 80663 bytes
## 1. Lambda

- `16:32:41`   Lambda exists — updating
- `16:32:46` ✅   ✓ updated justhodl-global-sovereign
## 2. EB rule + permissions

- `16:32:47`   rule already correct: global-sovereign-12h (cron(15 6,18 * * ? *))
- `16:32:47` ✅   ✓ target → justhodl-global-sovereign
- `16:32:47` ✅   ✓ added invoke permission
- `16:32:47` harvesting + seeding history…
- `16:34:02` ✅ barometer live: stress 47.4, worst Chile
- `16:34:02`   history_n: 1 (seeds at 1, grows twice daily)
- `16:34:02`   percentile: None (needs ≥3 points to compute)
- `16:34:02`   chg_7d: None · chg_30d: None (build over time)
- `16:34:02` ✅ history file written — 1 snapshot(s): {'date': '2026-07-16', 'stress': 47.4, 'avg_cds_bp': 22.9, 'worst_country': 'Chile', 'worst_cds_bp': 67.0}
