# ops 3834 — fleet audit: silently-neutral overlays

**Status:** success  
**Duration:** 67.6s  
**Finished:** 2026-07-24T23:30:31+00:00  

## Data

| active | artifacts_scanned | near_silent | overlay_fields | silent | sparse | stale_feed_drops |
|---|---|---|---|---|---|---|
| 0 | 994 | 2 | 3 | 0 | 1 | 2 |

## Log
## 1. Enumerate artifacts

- `23:29:24` ✅   994 artifacts under 4MB
## 2. Scan for overlay fields

- `23:30:31` ✅   scanned 994 artifacts (0 unreadable) · 3 overlay fields found
## 3. SILENT — present on every row, never once != 1.0

- `23:30:31` ✅   none
## 4. NEAR_SILENT — <10% active

- `23:30:31` ⚠   _preview/master-ranker.json        top_tickers.sector_flow_mult           1/25 = 4.0%
- `23:30:31` ⚠   _preview/master-ranker.json        top_tickers.risk_regime_mult           1/25 = 4.0%
## 5. Healthy overlays (context — what normal looks like)

## 6. Feeds excluded by staleness (where published)

- `23:30:31` ⚠   _preview/master-ranker.json: 2 feed(s) dropped -> ['options-confluence.json', 'scarcity-radar.json']
- `23:30:31` ✅ AUDIT COMPLETE — suspects ranked for judgement, nothing auto-changed
