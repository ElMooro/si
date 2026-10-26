# ops 6675 - switch off treasury-auto-updater (leave the daily-morn fan-out tick)

**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-10-26T15:44:11+00:00  

## Log
- `15:44:10` treasury-api reserved concurrency (the 2026-10-20 operator hold): 0
- `15:44:11` live manifest (generated_at 2026-09-02T01:17:54+00:00): treasury-auto-updater in ticks ['daily-morn']
- `15:44:11` ✅ treasury-auto-updater left ['daily-morn'] on the live manifest (backup s3://justhodl-ops-private-857687956942/backups/retire-2026-10-26/20261026T154411Z/manifest/fanout-manifest.json)
- `15:44:11` repo and live tick memberships agree
- `15:44:11` ✅ ops 6675 complete
