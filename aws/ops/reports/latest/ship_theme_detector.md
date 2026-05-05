# 1) Create or update Lambda

**Status:** success  
**Duration:** 11.3s  
**Finished:** 2026-05-05T13:29:57+00:00  

## Log
- `13:29:46`   zip size: 10,358b
- `13:29:46`   Creating new Lambda...
- `13:29:51` ✅   ✓ Lambda active, mod=2026-05-05T13:29:46.663+0000
# 2) Schedule daily 06:00 UTC

- `13:29:52`   ✓ Schedule wired
# 3) Smoke invoke — fetch 270 tickers and classify 70+ themes

- `13:29:57`   status: 200, duration: 4.8s
- `13:29:57`   n_themes:            0
- `13:29:57`   duration_s:          3.9
- `13:29:57`   phase_distribution:  {'DORMANT': 0, 'EMERGING': 0, 'ACCELERATING': 0, 'EXTENDED': 0, 'PEAKING': 0, 'COOLING': 0, 'DYING': 0}
- `13:29:57`   HOTTEST themes:      []
- `13:29:57`   TIER-2 hunt grounds: []
- `13:29:57`   EMERGING themes:     []
- `13:29:57`   DYING themes:        []
# 4) Verify S3 data/themes-detected.json

- `13:29:57`   v:                  1.0
- `13:29:57`   method:             thematic_etf_lifecycle_v1
- `13:29:57`   duration_s:         3.9
- `13:29:57`   fetch_stats:        {'n_tickers': 550, 'n_ok': 0, 'n_fail': 550, 'fetch_duration_s': 3.9}
- `13:29:57`   n_themes_classified:0
- `13:29:57`   phase_distribution: {'DORMANT': 0, 'EMERGING': 0, 'ACCELERATING': 0, 'EXTENDED': 0, 'PEAKING': 0, 'COOLING': 0, 'DYING': 0}
- `13:29:57` 
- `13:29:57`   ── HOTTEST THEMES (EXTENDED + ACCELERATING) ─────────────
- `13:29:57`   ── EMERGING THEMES (early entry zone) ─────────────
- `13:29:57`     (none currently emerging)
- `13:29:57` 
- `13:29:57`   ── DYING THEMES (avoid / consider short) ─────────────
- `13:29:57`     (none currently dying)
