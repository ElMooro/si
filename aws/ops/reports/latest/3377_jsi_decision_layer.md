# ops 3377 — JSI v1.9.0 decision layer E2E

**Status:** success  
**Duration:** 138.0s  
**Finished:** 2026-07-17T04:54:19+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:52:27` PASS  G1_engine_v190_settled — markers in deployed zip
- `04:52:42` PASS  G2_fresh_v190_v2 — gen=2026-07-17T04:52:33 v2=yes
- `04:52:42` FAIL  G3_atlas_sane — d9_1m_n=724 d9_3m=8.2 d2_3m=3.1 current_decile=2
- `04:52:42` PASS  G4_episodes — n=16 spans_2008=True spans_2020=True latest={'start': '2023-03-09', 'end': '2023-03-17', 'days': 7, 'peak': 73.1, 'peak_date': '2023-03-13', 'nasdaq_max_dd_pct': -1.8, 'nasdaq_fwd_63d_from_peak_pct': 17.1}
- `04:52:42` PASS  G5_layers_typed — vel21=-1.79 vz=-0.33 flare=False regime=NORMAL/73d div=NONE movers=yes(8)
- `04:54:19` PASS  G6_page_v2_live — markers=all
- `04:54:19` PASS  G7_sentinel_jsi_armed — jsi_regime marker in deployed sentinel
- `04:54:19` VERDICT: GAPS: G3_atlas_sane
