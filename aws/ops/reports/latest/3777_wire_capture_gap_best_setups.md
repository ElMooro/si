# ops 3777 — capture_gap overlay into best-setups

**Status:** failure  
**Duration:** 0.4s  
**Finished:** 2026-07-23T19:40:45+00:00  

## Error

```
SystemExit: 1
```

## Data

| capture_names | current_setups | forecast_overlap |
|---|---|---|
| 1771 | 0 | 0 |

## Log
## G0 — read the LIVE artifact, assert per-ROW fields

- `19:40:44` ✅ G0.container :: capture_gap container present
- `19:40:44` ✅ G0.all_rows :: all_rows n=1771
- `19:40:44` ✅ G0.row_ticker :: present on live rows
- `19:40:44` ✅ G0.row_capture_gap :: present on live rows
- `19:40:44` ✅ G0.row_global_capture_gap :: present on live rows
- `19:40:44` ✅ G0.row_tier :: present on live rows
- `19:40:44` ✅ G0.row_mcap_share_pct :: present on live rows
- `19:40:44` ✅ G0.row_undervaluation_score :: present on live rows
- `19:40:44` ✅ G0.catchup_present :: catchup_pct populated on live rows
- `19:40:44` ✅ G0.reads_chokepoint :: best-setups already loads chokepoint.json
- `19:40:44` ✅ G0.anchor :: splice anchor unique
- `19:40:45` ✗ G0.overlap_nonzero :: 0 setups will join (sample: [])
