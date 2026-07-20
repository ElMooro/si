# ops 3603 — global vol canaries + daily-depth fix

**Status:** success  
**Duration:** 500.6s  
**Finished:** 2026-07-20T22:55:49+00:00  

## Error

```
SystemExit: 0
```

## Log
- `22:47:29`   zip: 88742 bytes
## 1. Lambda

- `22:47:29`   Lambda exists — updating
- `22:47:33` ✅   ✓ updated justhodl-fifx-vol-migration
## 3. Smoke test

- `22:47:33`   invoking justhodl-fifx-vol-migration…
- `22:48:14` ✅   ✓ smoke test passed
- `22:48:14`     ok                       True
- `22:48:14`     state                    CALM
- `22:48:14`     spillover                -0.77
- `22:48:14`     fi_z                     -0.84
- `22:48:14`     fx_z                     -0.76
- `22:48:14`     eq_z                     0.01
- `22:48:47` PASS  G1_daily_depth — KOSPI n_px=7285 rlzd=79.76% z=2.73 (99.0p) · HSI n_px=9757 rlzd=21.53% z=-0.08 · asia_spill=2.72 state=ASIA_CANARY
- `22:48:47` PASS  G2_global_grid — indices_ok=10/10 breadth=10.0% elevated=['KOSPI'] leader={'name': 'KOSPI', 'z': 2.73} zs=[('KOSPI', 2.73), ('Shanghai', 0.92), ('Nikkei', 0.89), ('Sensex', 0.14), ('Bovespa', 0.1), ('DAX', 0.09)]
- `22:48:47` PASS  G3_deep_crisis — as_rows=2171 gb_rows=2175 from 1990-03-30 · asp: 97-98=3.38 ⭐2008=2.93 2020=3.6 · breadth gb: 2008=100.0% 2020=100.0% · today as=-0.08 asp=-0.09 gb=0.0%
- `22:55:49` FAIL  G4_page_served — {'GLOBAL VOL CANARIES': True, 'stress breadth': True, '22d3ee': False, 'KOSPI': True}
- `22:55:49` VERDICT: GAPS: G4_page_served
