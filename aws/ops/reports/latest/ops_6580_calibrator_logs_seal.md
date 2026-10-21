# ops 6580 - calibrator daily schedule; 30-day log retention; log-ingestion alarm

**Status:** success  
**Duration:** 33.5s  
**Finished:** 2026-10-21T00:18:33+00:00  

## Data

| alarm | calibrator_schedule | retention_set |
|---|---|---|
| justhodl-logs-ingestion-2gb-hour | cron(5 22 * * ? *) | 173 |

## Log
- `00:18:00` ✅ schedule justhodl-calibrator-daily created: cron(5 22 * * ? *) UTC
- `00:18:01` S3 schedule manifest: calibrator schedule added
- `00:18:01` S3 schedule manifest equals the committed file
- `00:18:33` ✅ log groups: 1245; retention set to 30 days on 173 that had none; 1072 kept their own
- `00:18:33` ✅ alarm justhodl-logs-ingestion-2gb-hour: INSUFFICIENT_DATA
- `00:18:33` ✅ done
