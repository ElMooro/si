# Wave 2 final audit

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-05-04T13:35:02+00:00  

## Log
# Lambdas live

- `13:35:02` ✅   ✓ justhodl-calibration-snapshot          state=Active   mem= 512MB  timeout=120s
- `13:35:02` ✅   ✓ justhodl-sector-rotation               state=Active   mem= 512MB  timeout=120s
- `13:35:02` ✅   ✓ justhodl-alert-router                  state=Active   mem= 256MB  timeout=120s
# Schedules wired

- `13:35:02` ✅   ✓ justhodl-calibration-snapshot-30min        rate(30 minutes)     state=ENABLED
- `13:35:02` ✅   ✓ justhodl-sector-rotation-6h                rate(6 hours)        state=ENABLED
- `13:35:02` ✅   ✓ justhodl-alert-router-30min                rate(30 minutes)     state=ENABLED
# S3 outputs producing data

- `13:35:02` ✅   ✓ data/calibration-snapshot.json               32,223b  modified=2026-05-04T13:19:17+00:00
- `13:35:02` ✅   ✓ data/sector-rotation.json                    13,441b  modified=2026-05-04T13:24:13+00:00
- `13:35:02` ✅   ✓ data/alert-history.json                       4,118b  modified=2026-05-04T13:33:11+00:00
- `13:35:02` ✅   ✓ alerts-state.json                               466b  modified=2026-05-04T13:33:11+00:00
# Frontend pages live (via GH Pages)

- `13:35:02` ✅   ✓ accuracy.html                  status=200  size=17,861b
- `13:35:02` ✅   ✓ sectors.html                   status=200  size=14,527b
# Alert router today

- `13:35:02`   total alerts in history: 8
- `13:35:02`   last run: 2026-05-04T13:33:10.035958+00:00
- `13:35:02`   by severity:
- `13:35:02`     HIGH: 5
- `13:35:02`     LOW: 1
- `13:35:02`     MEDIUM: 2
- `13:35:02`   by category:
- `13:35:02`     CORRELATION: 5
- `13:35:02`     SECTOR: 1
- `13:35:02`     SHORT_INTEREST: 2
