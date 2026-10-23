# ops 6627 - log-ingestion brake on the account-level metric (same alarm, ~$19/month -> $0.10)

**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-10-23T18:11:59+00:00  

## Data

| changed | hours_compared | state |
|---|---|---|
| True | 3 | OK |

## Log
- `18:11:58` current: Metrics Insights, state OK, actions ['arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts']
- `18:11:58` hourly IncomingBytes, Metrics Insights vs account metric (MB): [('15:00', 0.16, 0.22, True), ('16:00', 0.25, 0.25, True), ('17:00', 0.23, 0.23, True)]
- `18:11:58` ✅ equivalent: the account-level metric equals the Metrics Insights sum hour by hour
- `18:11:59` ✅ backup: s3://justhodl-ops-private-857687956942/backups/alarms/justhodl-logs-ingestion-2gb-hour-20261023T181158Z.json
- `18:11:59` ✅ put + read-back: standard alarm on AWS/Logs IncomingBytes Sum/3600s > 2 GiB, actions ['arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts'], state OK
