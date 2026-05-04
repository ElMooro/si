# Wave 3 Lambdas + S3 outputs

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-05-04T18:59:04+00:00  

## Log
- `18:59:02` ✅   ✓ justhodl-allocator                     state=Active   mod=2026-05-04T18:39:51.490+0000
- `18:59:02` ✅   ✓ justhodl-momentum-scanner              state=Active   mod=2026-05-04T18:55:16.000+0000
- `18:59:02` ✅   ✓ justhodl-wave-signal-logger            state=Active   mod=2026-05-04T18:19:53.000+0000
# Schedules

- `18:59:03`   ✗ justhodl-allocator-6h: An error occurred (ResourceNotFoundException) when calling the DescribeRule operation: Rule justhodl-allocator-6h does not exist on EventBus default.
- `18:59:03` ✅   ✓ justhodl-momentum-scanner-daily            cron(30 12 ? * MON-FRI *) state=ENABLED
- `18:59:03` ✅   ✓ justhodl-wave-signal-logger-6h             cron(30 0,6,12,18 ? * * *) state=ENABLED
# S3 outputs

- `18:59:03` ✅   ✓ data/allocator.json                             4,803b  mod=2026-05-04T18:40:03+00:00
- `18:59:03` ✅   ✓ data/momentum-scanner.json                     80,619b  mod=2026-05-04T18:55:35+00:00
- `18:59:03` ✅   ✓ data/sector-rotation.json                      13,441b  mod=2026-05-04T13:24:13+00:00
- `18:59:03` ✅   ✓ data/calibration-snapshot.json                 34,542b  mod=2026-05-04T18:41:45+00:00
- `18:59:03` ✅   ✓ data/alert-history.json                         5,247b  mod=2026-05-04T18:33:11+00:00
- `18:59:03`   ✗ data/flow-data.json: An error occurred (404) when calling the HeadObject operation: Not Found
- `18:59:03` ✅   ✓ data/vix-curve.json                               504b  mod=2026-05-04T18:10:57+00:00
- `18:59:03` ✅   ✓ data/insider-trades.json                       15,120b  mod=2026-05-04T18:03:58+00:00
- `18:59:03` ✅   ✓ data/earnings-tracker.json                     29,894b  mod=2026-05-04T17:46:35+00:00
- `18:59:03` ✅   ✓ data/macro-surprise.json                       11,259b  mod=2026-05-04T18:16:07+00:00
- `18:59:04` ✅   ✓ divergence/current.json                         6,275b  mod=2026-05-04T13:00:38+00:00
- `18:59:04` ✅   ✓ data/correlation-surface.json                  38,656b  mod=2026-05-04T15:00:56+00:00
- `18:59:04` ✅   ✓ data/auction-crisis.json                       11,576b  mod=2026-05-04T18:45:14+00:00
- `18:59:04` ✅   ✓ data/whats-changed.json                         1,657b  mod=2026-05-04T17:00:23+00:00
