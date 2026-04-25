# Step 85 — Fix edge threshold + build health.html

**Status:** success  
**Duration:** 9.0s  
**Finished:** 2026-04-25T01:05:34+00:00  

## Data

| api_url | dashboard_url | next_step |
|---|---|---|
| https://justhodl-dashboard-live.s3.amazonaws.com/health.html | https://justhodl-dashboard-live.s3-website-us-east-1.amazonaws.com/health.html | step 86 wires Telegram alerting + EB schedule |

## Log
## 1. Re-tune edge-data threshold based on observed reality

- `01:05:25` ⚠   Pattern not found; manual review
- `01:05:28` ✅   Re-deployed monitor: 6507 bytes
- `01:05:33`   Re-invoke status: 200
- `01:05:33`   System: red
- `01:05:33`   Counts: {'green': 24, 'yellow': 2, 'red': 1, 'info': 2, 'unknown': 0}
## 2. Upload health.html dashboard

- `01:05:34` ✅   Uploaded health.html (9972 bytes)
## 3. Verify health.html is in public-read bucket policy

- `01:05:34`   Adding health.html to PublicReadRootDashboardFiles statement
- `01:05:34` ✅   Updated bucket policy
- `01:05:34` Done
