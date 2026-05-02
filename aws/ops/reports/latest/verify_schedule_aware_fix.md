# Verify schedule-aware fix on 5 RED items

**Status:** success  
**Duration:** 7.1s  
**Finished:** 2026-05-02T21:35:45+00:00  

## Log
## Invoke health-monitor synchronously to trigger fresh check

- `21:35:44` ✅   invocation status: 200
## Read full dashboard.json from S3

- `21:35:45`   total components: 63
- `21:35:45`   status counts: green=55 yellow=4 red=2 info=2 unknown=0
## Status of the 5 previously-RED items

- `21:35:45`   [green   ] s3:repo-data.json
- `21:35:45`              Repo plumbing stress. repo-monitor every 30min weekdays.
- `21:35:45`   [green   ] s3:intelligence-report.json
- `21:35:45`              Cross-system synthesis. Heart of ai-chat + signal-logger.
- `21:35:45`   [red     ] lambda:justhodl-intelligence
- `21:35:45`              only 3 invocations in 24h (expected ≥4)
- `21:35:45`   [green   ] lambda:justhodl-nyfed-dealer-survey
- `21:35:45`              weekly schedule — 2 inv in last 8 days
- `21:35:45`   [green   ] lambda:justhodl-oecd-cli
- `21:35:45`              weekly schedule — 3 inv in last 8 days
## Summary

- `21:35:45`   Some still not green — see above
- `21:35:45` 
  Total RED across all 63 components: 2
- `21:35:45`   Remaining REDs:
- `21:35:45`     - lambda:justhodl-intelligence: only 3 invocations in 24h (expected ≥4)
- `21:35:45`     - lambda:justhodl-repo-monitor: only 5 invocations in 24h (expected ≥6)
