# ops 3816 — rotation-dashboard v1.0.0 (cross-asset spine)

**Status:** failure  
**Duration:** 18.0s  
**Finished:** 2026-07-24T20:20:40+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3816_rotation_dashboard.py", line 170, in main
    rep.kv("regime", regime)
TypeError: Report.kv() takes 1 positional argument but 3 were given

```

## Log
## G0. KEY CONTRACT — grep producers before consuming

- `20:20:22` ✅   nowcast_quadrant: 'nowcast_quadrant' present in justhodl-nowcast-desk
- `20:20:22` ✅   risk-regime score: 'score' present in justhodl-risk-regime
- `20:20:22` ✅   dollar chg_3m_pct: 'chg_3m_pct' present in justhodl-dollar-radar
- `20:20:22` ✅ G0 PASS — every consumed key exists in its producer
## 1. Inherit env from donors

- `20:20:23`   justhodl-industry-rotation: contributed nothing
- `20:20:23`   justhodl-asset-compass: contributed nothing
- `20:20:23`   justhodl-factor-regime: contributed nothing
- `20:20:23`   justhodl-equity-research: contributed ['POLYGON_API_KEY']
- `20:20:23` ✅   inherited ['POLYGON_API_KEY']
## 2. Deploy

- `20:20:23`   zip: 93872 bytes
## 1. Lambda

- `20:20:24`   Lambda exists — updating
- `20:20:30` ✅   ✓ updated justhodl-rotation-dashboard
## 3. Zip-settle

- `20:20:36` ✅   settled after 5s (State=Active)
## 4. Schedule

- `20:20:36` ✅   Scheduler already exists (ConflictException = success)
## 5. Invoke

- `20:20:40`   {'statusCode': 200, 'body': '{"ok": true, "scored": 37, "eligible": 20, "overweight": ["XLV", "IWD", "XLF", "EWJ", "RSP", "USO", "XLK", "IWM"]}'}
## 6. Verify live artifact

- `20:20:40` ✅   L1 regime resolved = STAGFLATION
- `20:20:40` ✅   L2 ratios >= 8 = 11
- `20:20:40` ✅   L3/L4 scored >= 25 = 37
- `20:20:40` ✅   trend gate discriminates eligible 20/37
- `20:20:40` ✅   RRG quadrants populated = {'LEADING': 7, 'IMPROVING': 2, 'WEAKENING': 4, 'LAGGING': 23}
- `20:20:40` ✅   overweight list non-empty = ['XLV', 'IWD', 'XLF', 'EWJ', 'RSP', 'USO', 'XLK', 'IWM']
- `20:20:40` ✅   hysteresis field present 
- `20:20:40` ✅   caveats shipped 
