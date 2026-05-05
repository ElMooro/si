
# 1) Read compound-signals.json data shape

- `23:29:51`     schema_version: 2
- `23:29:51`     generated_at:   2026-05-05T22:58:08+00:00
- `23:29:51`     feed_stats:     {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 22, "eps_velocity": 25}
- `23:29:51`     stats:          {"n_total_names": 171, "n_multi_signal": 7, "n_3_plus": 1, "n_compound_over_200": 5, "n_compound_over_300": 1}
- `23:29:51`   
- `23:29:51`     ── FCX full record ──
- `23:29:51`     symbol:         FCX
- `23:29:51`     n_systems:      3
- `23:29:51`     systems:        ['eps_velocity', 'nobrainers', 'smart_money']
- `23:29:51`     scores:         {'nobrainers': 76.2, 'smart_money': 28.4, 'eps_velocity': 79.3}
- `23:29:51`     compound_score: 367.8
- `23:29:51`     details keys:   ['nobrainers', 'smart_money', 'eps_velocity']

# 2) Read compound-signals.html — verify it'll render FCX correctly

- `23:29:52`     status: 200, size: 18,086b
- `23:29:52`   
- `23:29:52`     ── page JS expectations ──
- `23:29:52`       ✓ data/compound-signals.json           fetches compound data
- `23:29:52`       ✓ tier3-grid                           tier-3 section grid
- `23:29:52`       ✓ tier2-grid                           tier-2 section grid
- `23:29:52`       ✓ compound_score                       uses compound_score field
- `23:29:52`       ✓ n_systems                            uses n_systems field
- `23:29:52`       ✓ renderCard                           card render function
- `23:29:52`       ✓ renderDetail                         detail per-system renderer

# 3) Simulate what the FCX card will display

- `23:29:52`     Based on the data structure, the FCX card will show:
- `23:29:52`       • Header: 'FCX' with score badge '367'
- `23:29:52`       • Pills: [eps_velocity] [nobrainers] [smart_money]
- `23:29:52`       • eps_velocity block: {"flag": "HIGH_VELOCITY_TIER_B", "fy2_lift_pct": 47.2, "fwd_rev_growth_pct": 21.5, "company": "Freeport-McMoRan Inc."}
- `23:29:52`       • nobrainers block: {"theme": "PICK", "tier": 2, "flag": "TIER_B_HIGH_CONVICTION", "name": "Freeport-McMoRan Inc."}
- `23:29:52`       • smart_money block: {"signal_types": ["LEGEND_FUND_BUY"], "n_buyers": 2, "n_sellers": 7, "legend_buyers": ["LONE_PINE"], "name": "Freeport-McMoRan"}