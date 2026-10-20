# ops 6571 - old copies capped by count: enable the bucket-wide keep-10 rule, retire the 1-day age rules

**Status:** success  
**Duration:** 6.2s  
**Finished:** 2026-10-20T23:34:04+00:00  

## Data

| backup | changes | readback_mismatches | rules_after | rules_before | unexpected_rules | untouched_rules_identical |
|---|---|---|---|---|---|---|
| s3://justhodl-ops-private-857687956942/backups/lifecycle/justhodl-dashboard-live/20261020T233400Z.*.json | 4 | 0 | 11 | 11 | 0 | 7 |

## Log
- `23:33:58` census 20261020T224932Z.json: 239 prefixes; prefixes with keys > 10 old versions: [('WITHHELD', 3089, 195), ('index/', 1, 56), ('equity-research/', 62, 93), ('equity-research-history/', 2, 15), ('etf-flows/', 293, 80), ('data/', 1, 6657080), ('system-events/', 1, 573713), ('calibration/', 3, 91), ('screener/', 14, 1836), ('data-census/', 1, 83), ('analytics/', 6, 90), ('edgar-insiders/', 52, 76), ('', 8, 46005), ('data/ops/', 1, 27), ('13f/', 2, 25), ('domain-barometers/', 1, 44), ('etf-constituents-v2/', 44, 14), ('cot/', 36, 13), ('data/a2a/', 1, 9994), ('errors/', 1, 2199), ('equity-prewarm/', 1, 76), ('chokepoint/', 1, 149), ('spx-beaters/', 1, 14), ('config/', 1, 15), ('geo/', 1, 75), ('macro/', 1, 78), ('flow-anomalies/', 2, 77), ('portfolio/', 11, 2790), ('spx-ma/', 1, 56), ('symbol-resolver/', 1, 75), ('factory/', 2, 9338), ('_health/', 6, 670), ('reports/', 1, 131), ('sentiment/', 1, 158), ('backtest/', 3, 77), ('estimate-revisions/', 1, 143), ('macro-attribution/', 1, 76), ('credit/', 1, 77), ('boom/', 1, 75), ('readthrough/', 1, 137), ('regime/', 2, 46), ('signals/', 6, 2640), ('risk/', 1, 47), ('divergence/', 2, 45), ('plumbing-composite/', 1, 53), ('air/', 1, 82), ('state/', 1, 77), ('learning/', 2, 76), ('asia/', 2, 77), ('data/_health/', 1, 16), ('telegram/', 1, 77)]
- `23:34:00` before: archive-to-glacier-deep-after-90d [Enabled] prefix='archive/': abort-mpu {"DaysAfterInitiation": 7}; transitions
- `23:34:00` before: expire-old-versions-after-30d [Enabled] prefix='': abort-mpu {"DaysAfterInitiation": 7}
- `23:34:00` before: expire-screener-snapshots-30d [Enabled] prefix='screener/snapshots/': expire-current {"Days": 30}
- `23:34:00` before: jh-noncurrent-14d [Enabled] prefix='': abort-mpu {"DaysAfterInitiation": 3}
- `23:34:00` before: justhodl-e9-warm-ia [Enabled] prefix='data/warm/': transitions
- `23:34:00` before: justhodl-e9-raw-glacier-ir [Enabled] prefix='data/raw/': transitions
- `23:34:00` before: justhodl-e9-attic-ia [Enabled] prefix='data/attic/': transitions
- `23:34:00` before: ops5027-purge-dead-versions-providers [Enabled] prefix='data/providers/': expire-current {"ExpiredObjectDeleteMarker": true}; expire-old {"NoncurrentDays": 1}; abort-mpu {"DaysAfterInitiation": 1}
- `23:34:00` before: ops5164-purge-dead-versions-data [Enabled] prefix='data/': expire-current {"ExpiredObjectDeleteMarker": true}; expire-old {"NoncurrentDays": 1}
- `23:34:00` before: ops6520-purge-dead-versions-system-events [Enabled] prefix='system-events/': expire-current {"ExpiredObjectDeleteMarker": true}; expire-old {"NoncurrentDays": 1}
- `23:34:00` before: ops6541-bound-noncurrent-versions [Disabled] prefix='': expire-old {"NewerNoncurrentVersions": 10, "NoncurrentDays": 1}
- `23:34:00` change: ops5027-purge-dead-versions-providers: 1-day age expiry removed, delete-marker sweep kept
- `23:34:00` change: ops5164-purge-dead-versions-data: 1-day age expiry removed, delete-marker sweep kept
- `23:34:00` change: ops6520-purge-dead-versions-system-events: 1-day age expiry removed, delete-marker sweep kept
- `23:34:00` change: ops6541-bound-noncurrent-versions: Disabled -> Enabled
- `23:34:04` after: archive-to-glacier-deep-after-90d [Enabled] prefix='archive/': abort-mpu {"DaysAfterInitiation": 7}; transitions
- `23:34:04` after: expire-old-versions-after-30d [Enabled] prefix='': abort-mpu {"DaysAfterInitiation": 7}
- `23:34:04` after: expire-screener-snapshots-30d [Enabled] prefix='screener/snapshots/': expire-current {"Days": 30}
- `23:34:04` after: jh-noncurrent-14d [Enabled] prefix='': abort-mpu {"DaysAfterInitiation": 3}
- `23:34:04` after: justhodl-e9-warm-ia [Enabled] prefix='data/warm/': transitions
- `23:34:04` after: justhodl-e9-raw-glacier-ir [Enabled] prefix='data/raw/': transitions
- `23:34:04` after: justhodl-e9-attic-ia [Enabled] prefix='data/attic/': transitions
- `23:34:04` after: ops5027-purge-dead-versions-providers [Enabled] prefix='data/providers/': expire-current {"ExpiredObjectDeleteMarker": true}; abort-mpu {"DaysAfterInitiation": 1}
- `23:34:04` after: ops5164-purge-dead-versions-data [Enabled] prefix='data/': expire-current {"ExpiredObjectDeleteMarker": true}
- `23:34:04` after: ops6520-purge-dead-versions-system-events [Enabled] prefix='system-events/': expire-current {"ExpiredObjectDeleteMarker": true}
- `23:34:04` after: ops6541-bound-noncurrent-versions [Enabled] prefix='': expire-old {"NewerNoncurrentVersions": 10, "NoncurrentDays": 1}
- `23:34:04` ✅ old copies are now capped at the 10 newest per file, bucket-wide; current objects untouched by any rule
