# Pre-cleanup AUDIT — verify every candidate before any delete

**Status:** success  
**Duration:** 1.1s  
**Finished:** 2026-04-23T17:38:08+00:00  

## Data

| candidates | keepable | missing | to_delete |
|---|---|---|---|
| 13 | 0 | 0 | 13 |

## Log
## 1. For each candidate: state + schedule + all targets

- `17:38:07`   justhodl-crypto-intel-schedule
- `17:38:07`     state: ENABLED
- `17:38:07`     schedule: rate(15 minutes)
- `17:38:07`     targets: ['lambda:justhodl-crypto-intel']
- `17:38:07`   justhodl-ml-schedule
- `17:38:07`     state: ENABLED
- `17:38:07`     schedule: rate(4 hours)
- `17:38:07`     targets: ['lambda:justhodl-ml-predictions']
- `17:38:07`   justhodl-edge-6h
- `17:38:07`     state: ENABLED
- `17:38:07`     schedule: rate(6 hours)
- `17:38:07`     targets: ['lambda:justhodl-edge-engine']
- `17:38:07`   justhodl-daily-8am
- `17:38:07`     state: ENABLED
- `17:38:07`     schedule: cron(0 13 * * ? *)
- `17:38:07`     targets: ['lambda:justhodl-daily-report-v3']
- `17:38:07`   justhodl-daily-v3
- `17:38:07`     state: ENABLED
- `17:38:07`     schedule: cron(0 13 * * ? *)
- `17:38:07`     targets: ['lambda:justhodl-daily-report-v3']
- `17:38:07`   justhodl-morning-brief-daily
- `17:38:07`     state: ENABLED
- `17:38:07`     schedule: cron(0 13 * * ? *)
- `17:38:07`     targets: ['lambda:justhodl-morning-intelligence']
- `17:38:07`   justhodl-8am
- `17:38:07`     state: ENABLED
- `17:38:07`     schedule: cron(0 13 * * ? *)
- `17:38:07`     targets: ['lambda:justhodl-email-reports']
- `17:38:07`   liquidity-critical-monitor
- `17:38:07`     state: DISABLED
- `17:38:07`     schedule: rate(15 minutes)
- `17:38:07`     targets: ['lambda:global-liquidity-agent-v2']
- `17:38:08`   liquidity-daily-8am
- `17:38:08`     state: DISABLED
- `17:38:08`     schedule: cron(0 13 * * ? *)
- `17:38:08`     targets: ['lambda:global-liquidity-agent-v2']
- `17:38:08`   liquidity-daily-report
- `17:38:08`     state: DISABLED
- `17:38:08`     schedule: cron(0 13 * * ? *)
- `17:38:08`     targets: ['lambda:global-liquidity-agent-v2']
- `17:38:08`   liquidity-daily-report-v2
- `17:38:08`     state: DISABLED
- `17:38:08`     schedule: cron(0 12 * * ? *)
- `17:38:08`     targets: ['lambda:global-liquidity-agent-v2']
- `17:38:08`   liquidity-hourly-v2
- `17:38:08`     state: DISABLED
- `17:38:08`     schedule: rate(1 hour)
- `17:38:08`     targets: ['lambda:global-liquidity-agent-v2']
- `17:38:08`   liquidity-news-v2
- `17:38:08`     state: DISABLED
- `17:38:08`     schedule: rate(15 minutes)
- `17:38:08`     targets: ['lambda:global-liquidity-agent-v2']
## 2. Check fmp-stock-picks-daily — suspected identical duplicate

- `17:38:08`   Single rule exists: cron(0 12 ? * MON-FRI *)
- `17:38:08`   (My earlier listing showed it twice — likely just a display artifact)
## 3. Summary

- `17:38:08`   Candidates reviewed: 13
- `17:38:08`   Already missing: 0
- `17:38:08`   Keep (non-Lambda targets): 0
- `17:38:08`   Safe to delete: 13
- `17:38:08` 
- `17:38:08`   Will delete on next run:
- `17:38:08`     - justhodl-crypto-intel-schedule
- `17:38:08`     - justhodl-ml-schedule
- `17:38:08`     - justhodl-edge-6h
- `17:38:08`     - justhodl-daily-8am
- `17:38:08`     - justhodl-daily-v3
- `17:38:08`     - justhodl-morning-brief-daily
- `17:38:08`     - justhodl-8am
- `17:38:08`     - liquidity-critical-monitor
- `17:38:08`     - liquidity-daily-8am
- `17:38:08`     - liquidity-daily-report
- `17:38:08`     - liquidity-daily-report-v2
- `17:38:08`     - liquidity-hourly-v2
- `17:38:08`     - liquidity-news-v2
- `17:38:08` 
- `17:38:08`   KEEPING (for the record):
- `17:38:08`     - justhodl-v9-auto-refresh: THE main 5-min pipeline — never delete
- `17:38:08`     - justhodl-v9-morning: Market open snapshot (9 AM ET / 13 UTC MON-FRI)
- `17:38:08`     - justhodl-v9-evening: Market close snapshot (7 PM ET / 23 UTC MON-FRI)
- `17:38:08`     - justhodl-crypto-15min: Keep crypto-intel 15min trigger
- `17:38:08`     - justhodl-ml-predictions-schedule: Keep ml-predictions 4h trigger
- `17:38:08`     - justhodl-edge-engine-6h: Keep edge-engine 6h trigger (name matches Lambda)
- `17:38:08`     - secretary-4h-scan: Keep Secretary 4h trigger
- `17:38:08`     - justhodl-signal-logger-6h: Keep signal logger
- `17:38:08`     - justhodl-stock-screener-4h: Keep screener
- `17:38:08`     - justhodl-outcome-checker-weekly: Keep weekly Sunday outcomes
- `17:38:08`     - justhodl-calibrator-weekly: Keep weekly Sunday calibration
- `17:38:08`     - justhodl-khalid-metrics-refresh: Keep Khalid metrics daily
- `17:38:08`     - cftc-cot-weekly-update: Keep CFTC Friday update
- `17:38:08` Done — AUDIT ONLY. No deletes performed.
