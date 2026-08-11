# ops 4594 — FRED 100/min + fleet-red closure

**Status:** failure  
**Duration:** 688.2s  
**Finished:** 2026-08-11T00:00:21+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. The knob

- `23:48:53`   prior /justhodl/fred/rate-ceiling = 100
- `23:48:53` ✅   [knob] ceiling now 100 req/min (AIMD climbs to it on clean windows, backs off on any 429, halts on 403 — Khalid-approved raise)
## 2. Drain kick + advance check

- `23:48:54`   fred-catalog kicked (checkpoint was 2026-08-10T23:45:14+00:00)
- `23:53:02` ✗   [drain] CONTRACT MISS — checkpoint advanced (2026-08-10T23:45:14+00:00 → 2026-08-10T23:45:14+00:00)
- `23:53:02` ✅   [drain] no block after raise (blocked_at=None)
- `23:53:02`     rate_rpm = 42.0
- `23:53:02`     throttled_429 = 261
- `23:53:03`   remaining=223278; projected at ceiling-100 effective ~4920/h → ETA ~45.4 h (~1.9 days). The health strip shows the real number within the hour; AIMD finds the true sustainable pace.
## 3. 4592 reds — deploy evidence + force redeploy

- `23:53:03`   catalyst-skew-premove LastModified=2026-08-10T23:20:52.000+0000
- `23:53:04`     cw last-90m: 4 lines, 0 error-sigs
- `23:53:11`     force-redeployed from repo source
- `23:53:11`   failed-pattern-reversal LastModified=2026-08-10T23:21:22.000+0000
- `23:53:12`     cw last-90m: 5 lines, 0 error-sigs
- `23:53:18`     force-redeployed from repo source
## 4. Re-gate the two

- `00:00:21` ⚠   justhodl-catalyst-skew-premove did not refresh in 420s
- `00:00:21` ⚠   justhodl-failed-pattern-reversal did not refresh in 420s
- `00:00:21` ✗   [catalyst-skew-premove] CONTRACT MISS — data_sufficiency published (state=QUIET, ds=None)
- `00:00:21` ✗   [failed-pattern-reversal] CONTRACT MISS — data_sufficiency published (state=QUIET, ds=None)
## verdict

- `00:00:21` ✗ rate raise / red closure: 5 red
