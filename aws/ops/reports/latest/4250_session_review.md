# ops 4250 — session review (4227-4249)

**Status:** failure  
**Duration:** 684.2s  
**Finished:** 2026-08-01T19:36:31+00:00  

## Error

```
SystemExit: FAILS: backfill incomplete: 106330 remaining; drift 1; coherence: An error occurred (404) when calling the HeadObject operation: Not Found
```

## Data

| artifact_change | cadence_h | copied | dest_now | failed | fn | prior_7d | s3_consumers | section | source | ssm_consumers | topic | wired |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 4.0 |  |  |  | justhodl-polygon-options-flow | 30 |  | silenced |  |  |  |  |
|  | 4.0 |  |  |  | justhodl-trade-tickets | 31 |  | silenced |  |  |  |  |
|  |  | 5000 | 5006 | 0 |  |  |  | crr_backfill | 111336 |  |  |  |
|  |  |  |  |  |  |  |  | alarms |  |  | arn:aws:sns:us-east-1:857687956942:jh-ops-alerts | 3 |
| additive |  |  |  |  |  |  | ~10 | ssm_containment |  | 0 |  |  |

## Log
## R1. Regression sweep — did today's surgery silence anything?

- `19:25:07` functions with declared cadence <= 6h: 74
- `19:25:09` active-before, silent-in-8h: 2 (of which cadence<=1h: 0)
- `19:25:09` ⚠    justhodl-polygon-options-flow              cadence=4.0h prior7d=30 now=0
- `19:25:09` ⚠    justhodl-trade-tickets                     cadence=4.0h prior7d=31 now=0
## R2. CRR backfill — replication is forward-only

- `19:25:31` source objects: 111336 | already in us-west-2: 6 | missing: 111330
- `19:26:29`    … 400 copied
- `19:27:19`    … 800 copied
- `19:28:10`    … 1200 copied
- `19:29:01`    … 1600 copied
- `19:29:51`    … 2000 copied
- `19:30:43`    … 2400 copied
- `19:31:34`    … 2800 copied
- `19:32:25`    … 3200 copied
- `19:33:15`    … 3600 copied
- `19:34:06`    … 4000 copied
- `19:34:57`    … 4400 copied
- `19:35:47`    … 4800 copied
- `19:36:15` copied=5000 failed=0 | destination now holds 5006 of 111336
- `19:36:15` ⚠ 106330 objects still to converge (replication covers everything written from today forward)
## R3. Give the alarms someone to call

- `19:36:15` ✅ SNS topic arn:aws:sns:us-east-1:857687956942:jh-ops-alerts
- `19:36:16` ✅ subscribed raa*** — AWS sent a confirmation email; alarms deliver only after it is clicked
- `19:36:16` ✅    justhodl-integrity-new-defects -> jh-ops-alerts
- `19:36:16` ✅    justhodl-schedule-drift -> jh-ops-alerts
- `19:36:16` ✅    justhodl-contract-sev1 -> jh-ops-alerts
## R4. Mirror the contract registry into git

- `19:36:17` ✅ engine-contracts.json mirrored — 866 contracts, 760 cadence-bounded
## R5. SSM pointer containment (verified earlier, recorded)

- `19:36:17` The scorecard SSM value became a pointer today for payloads >8KB. Repo-wide grep: ZERO functions read /justhodl/calibration/scorecard — the ~10 real consumers (conviction-engine, engine-trust, apex-fusion, proven-portfolio, …) read data/signal-scorecard.json, which was extended ADDITIVELY only. Suspected contract break: cleared with evidence, no action needed.
## R6. Control-plane coherence

- `19:36:17` ✅    justhodl-fleet-integrity-weekly        cron(0 8 ? * MON *)    state=ENABLED targets=1
- `19:36:17` ✅    justhodl-d1-scan-daily                 cron(0 5 * * ? *)      state=ENABLED targets=1
- `19:36:17` ✅    justhodl-schedule-reconciler-daily     cron(30 7 * * ? *)     state=ENABLED targets=1
- `19:36:17` ✅    justhodl-contract-gate-daily           cron(0 13 * * ? *)     state=ENABLED targets=1
- `19:36:17` ✅    jh-clone-alpha-backfill-weekly         cron(30 8 ? * MON *)   state=ENABLED targets=1
- `19:36:31` ✗ live reconciler drift = 1
- `19:36:31` ✅    integrity    artifact 3.5h old
- `19:36:31` ✅    contracts    artifact 0.2h old
## RESULT

- `19:36:31` ✗   backfill incomplete: 106330 remaining
- `19:36:31` ✗   drift 1
- `19:36:31` ✗   coherence: An error occurred (404) when calling the HeadObject operation: Not Found
