# ops 6528 - error monitor hourly + freeze verification

**Status:** success  
**Duration:** 24.9s  
**Finished:** 2026-10-16T23:10:45+00:00  

## Log
## Error monitor cadence

- `23:10:20` ✅ rule fleet-error-monitor-5min: rate(5 minutes) -> rate(1 hour) (state ENABLED, 1 target(s) kept)
- `23:10:21` ✅ S3 schedule manifest: 1 entry updated
## Freeze check

- `23:10:22` ✅ governor: {"mode": "enforce", "stage": "FREEZE", "stage_since": "2026-10-16T22:53:03.699511+00:00", "held": {"FREEZE/lambda": 914, "FREEZE/smasync": 1}, "cap": 3500.0, "cap_source": "override:2026-10"}
- `23:10:23` ledger entries not applied: [{"key": "smasync:endpoint/jh-owned-coder-async/variant/owned", "error": "ValidationException", "status": "failed"}]
- `23:10:34` plan FREEZE now: {}
- `23:10:45` ✅ functions invoking 22:55-23:10 UTC: {"justhodl-spend-governor": 1}
- `23:10:45` account throttles last hour (scheduled runs refused): 29
- `23:10:45` ✅ wrote aws/ops/reports/6528.json
