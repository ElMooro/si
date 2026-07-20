# ops 3604 — Asia date-tolerant join + page forensic

**Status:** success  
**Duration:** 492.2s  
**Finished:** 2026-07-20T23:06:10+00:00  

## Error

```
SystemExit: 0
```

## Log
- `22:57:58`   zip: 88910 bytes
## 1. Lambda

- `22:57:58`   Lambda exists — updating
- `22:58:03` ✅   ✓ updated justhodl-fifx-vol-migration
## 3. Smoke test

- `22:58:03`   invoking justhodl-fifx-vol-migration…
- `22:58:35` ✅   ✓ smoke test passed
- `22:58:35`     ok                       True
- `22:58:35`     state                    CALM
- `22:58:35`     spillover                -0.77
- `22:58:35`     fi_z                     -0.84
- `22:58:35`     fx_z                     -0.76
- `22:58:35`     eq_z                     0.01
- `22:59:09` PASS  G1_join_fixed — today deep as=2.73 asp=2.72 gb=10.0% vs main kospi_z=2.73 breadth=10.0% state=ASIA_CANARY
- `23:06:10` FAIL  G2_page_cyan — ABSENT · len=38200 has_grid=True has_asia_node=True n_scripts=21
- `23:06:10` VERDICT: GAPS: G2_page_cyan
