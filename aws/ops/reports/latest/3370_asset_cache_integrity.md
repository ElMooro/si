# ops 3370 — content-addressed shared assets, fleet gates

**Status:** success  
**Duration:** 312.6s  
**Finished:** 2026-07-17T03:18:19+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:18:17` FAIL  G1_pages_stamped_expected — {"/why.html": "ok", "/index.html": "ok", "/capital-flow.html": "ok", "/chart-pro.html": "ok"}
- `03:18:19` FAIL  G2_bytes_identity — jh-nav-drawer.js:70248786 jh-page-ai.js:a62c53e1 auth.js:00f49db2 auth-config.js:ok interp-kit.js:c97a5881 jh-enhance.js:2d1133d3 cmdk.js:05f5380a jh-wire.js:83773466 jh-theme.css:ok
- `03:18:19` PASS  G3_auth_plan_self_delivered — http 200 marker=True
- `03:18:19` FAIL  G4_drawer_dynamic_dep_baked — http 200 baked=False
- `03:18:19` VERDICT: GAPS: G1_pages_stamped_expected,G2_bytes_identity,G4_drawer_dynamic_dep_baked
