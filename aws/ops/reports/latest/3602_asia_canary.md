# ops 3602 — Asia canary leg (KOSPI/HSI vol)

**Status:** success  
**Duration:** 362.5s  
**Finished:** 2026-07-20T22:43:55+00:00  

## Error

```
SystemExit: 0
```

## Log
- `22:37:53`   zip: 87568 bytes
## 1. Lambda

- `22:37:53`   Lambda exists — updating
- `22:37:57` ✅   ✓ updated justhodl-fifx-vol-migration
## 3. Smoke test

- `22:37:57`   invoking justhodl-fifx-vol-migration…
- `22:38:11` ✅   ✓ smoke test passed
- `22:38:11`     ok                       True
- `22:38:11`     state                    CALM
- `22:38:11`     spillover                -0.77
- `22:38:11`     fi_z                     -0.84
- `22:38:11`     fx_z                     -0.76
- `22:38:11`     eq_z                     0.01
- `22:38:24` PASS  G1_asia_legs — KOSPI rlzd=208.67% z=1.96 (95.2p, 336 pts) · HSI rlzd=106.31% z=-1.29 (140 pts, VHSI=None) · asia_z=1.96 asia_spill=1.95 state=ASIA_CANARY
- `22:38:24` FAIL  G2_deep_crisis — asia rows=49/2212 from 2003-10-31 · asp maxima: 97-98=None ⭐2008-09=0.62 2020=-0.55 · today asp=None
- `22:43:55` FAIL  G3_page_served — served: ASIA node + cyan canary overlay + state chip, card still top
- `22:43:55` VERDICT: GAPS: G2_deep_crisis,G3_page_served
