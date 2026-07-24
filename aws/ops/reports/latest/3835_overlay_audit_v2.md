# ops 3835 — fleet audit v2: silently-neutral overlays

**Status:** success  
**Duration:** 255.4s  
**Finished:** 2026-07-24T23:44:28+00:00  

## Data

| active | artifacts_scanned | near_silent | overlay_fields | silent | sparse | stale_feed_drops |
|---|---|---|---|---|---|---|
| 18 | 1998 | 2 | 29 | 7 | 2 | 2 |

## Log
## 1. Enumerate artifacts

- `23:40:13`   force-included data/master-ranker.json (must-scan, size-exempt)
- `23:40:13`   force-included data/best-setups.json (must-scan, size-exempt)
- `23:40:14` ✅   1998 artifacts (cap 30MB, 2 size-exempt)
## 2. Scan for overlay fields

- `23:44:28` ✅   scanned 1998 artifacts (0 unreadable) · 29 overlay fields found
## 3. SILENT — present on every row, never once != 1.0

- `23:44:28` ⚠   best-setups.json                   top_setups.khalid_panel_multiplier    0/50 active
- `23:44:28` ⚠   best-setups.json                   top_setups.risk_regime_mult           0/50 active
- `23:44:28` ⚠   best-setups.json                   structural_chokepoints.khalid_panel_multiplier    0/30 active
- `23:44:28` ⚠   best-setups.json                   structural_chokepoints.risk_regime_mult           0/30 active
- `23:44:28` ⚠   master-ranker.json                 top_tickers.risk_regime_mult           0/25 active
- `23:44:28` ⚠   best-setups.json                   structural_at_trough.khalid_panel_multiplier    0/9 active
- `23:44:28` ⚠   best-setups.json                   structural_at_trough.risk_regime_mult           0/9 active
## 4. NEAR_SILENT — <10% active

- `23:44:28` ⚠   _preview/master-ranker.json        top_tickers.risk_regime_mult           1/25 = 4.0%
- `23:44:28` ⚠   _preview/master-ranker.json        top_tickers.sector_flow_mult           1/25 = 4.0%
## 5. Healthy overlays (context — what normal looks like)

- `23:44:28`   best-setups.json                   top_setups.confluence_mult            100.0%
- `23:44:28`   best-setups.json                   structural_chokepoints.rotation_mult              96.7%
- `23:44:28`   best-setups.json                   structural_chokepoints.nowcast_regime_mult        96.7%
- `23:44:28`   best-setups.json                   structural_chokepoints.industry_mult              96.7%
- `23:44:28`   best-setups.json                   structural_at_trough.rotation_mult              88.9%
- `23:44:28`   best-setups.json                   structural_at_trough.nowcast_regime_mult        88.9%
- `23:44:28`   best-setups.json                   structural_at_trough.industry_mult              88.9%
- `23:44:28`   best-setups.json                   top_setups.rotation_mult              88.0%
- `23:44:28`   best-setups.json                   top_setups.nowcast_regime_mult        88.0%
- `23:44:28`   best-setups.json                   top_setups.industry_mult              88.0%
## 6. Feeds excluded by staleness (where published)

- `23:44:28` ⚠   _preview/master-ranker.json: 2 feed(s) dropped -> ['options-confluence.json', 'scarcity-radar.json']
## 0. SELF-VALIDATION — did the audit see what we know is there?

- `23:44:28` ✅   master-ranker.json: scanned
- `23:44:28` ✅   best-setups.json: scanned
- `23:44:28` ✅   known overlays rediscovered: ['nowcast_regime_mult', 'risk_regime_mult', 'rotation_mult']
- `23:44:28` ✅ AUDIT COMPLETE — suspects ranked for judgement, nothing auto-changed
