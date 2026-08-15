# ops 4711 — re-prioritize ICE-first, drive to completion this session

**Status:** failure  
**Duration:** 979.1s  
**Finished:** 2026-08-15T18:09:11+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Live state (not assumed — checking now)

- `17:52:52`   catalog=225 done=47 status=converging
## 2. Re-prioritize: BAML/ICE series first

- `17:52:52`   reordered: 192 BAML first, 33 other after
- `17:52:52`   BAML already done before this run: 45/192
## 3. Drive repeated invocations until ICE converges or time runs out

- `17:54:57`   round 1: {"ok": true, "pulled": 25, "failed": 35, "row_cap_hits": 0, "done": 72, "catalog": 225, "status": "converging", "mean_agree_pct": 100.0}
- `17:54:57`     BAML progress: 70/192
- `17:56:59`   round 2: {"ok": true, "pulled": 15, "failed": 45, "row_cap_hits": 0, "done": 87, "catalog": 225, "status": "converging", "mean_agree_pct": 100.0}
- `17:56:59`     BAML progress: 85/192
- `17:59:03`   round 3: {"ok": true, "pulled": 7, "failed": 53, "row_cap_hits": 0, "done": 94, "catalog": 225, "status": "converging", "mean_agree_pct": 100.0}
- `17:59:03`     BAML progress: 92/192
- `18:01:03`   round 4: {"ok": true, "pulled": 0, "failed": 60, "row_cap_hits": 0, "done": 94, "catalog": 225, "status": "converging", "mean_agree_pct": 100.0}
- `18:01:03`     BAML progress: 92/192
- `18:03:03`   round 5: {"ok": true, "pulled": 0, "failed": 60, "row_cap_hits": 0, "done": 94, "catalog": 225, "status": "converging", "mean_agree_pct": 100.0}
- `18:03:03`     BAML progress: 92/192
- `18:05:06`   round 6: {"ok": true, "pulled": 0, "failed": 60, "row_cap_hits": 0, "done": 94, "catalog": 225, "status": "converging", "mean_agree_pct": 100.0}
- `18:05:06`     BAML progress: 92/192
- `18:07:05`   round 7: {"ok": true, "pulled": 0, "failed": 60, "row_cap_hits": 0, "done": 94, "catalog": 225, "status": "converging", "mean_agree_pct": 100.0}
- `18:07:05`     BAML progress: 92/192
- `18:09:07`   round 8: {"ok": true, "pulled": 0, "failed": 60, "row_cap_hits": 0, "done": 94, "catalog": 225, "status": "converging", "mean_agree_pct": 100.0}
- `18:09:07`     BAML progress: 92/192
## 4. Final tally

- `18:09:11`   ICE/BAML final: 92/192 done, 60 in failures dict (rounds=8, elapsed=979s)
- `18:09:11`   mean cross-check agreement on ICE series: 100.0% (n=92)
- `18:09:11`     miss: BAMLEM1BRRAAA2ACRPIEY (empty)
- `18:09:11`     miss: BAMLEM1BRRAAA2ACRPIOAS (empty)
- `18:09:11`     miss: BAMLEM1BRRAAA2ACRPISYTW (empty)
- `18:09:11`     miss: BAMLEM1BRRAAA2ACRPITRIV (empty)
- `18:09:11`     miss: BAMLEM1RAAA2ALCRPIUSEY (empty)
- `18:09:11`     miss: BAMLEM1RAAA2ALCRPIUSOAS (empty)
- `18:09:11`     miss: BAMLEM1RAAA2ALCRPIUSSYTW (empty)
- `18:09:11`     miss: BAMLEM1RAAA2ALCRPIUSTRIV (empty)
- `18:09:11`     miss: BAMLEM2BRRBBBCRPIOAS (empty)
- `18:09:11`     miss: BAMLEM2BRRBBBCRPISYTW (empty)
## verdict

- `18:09:11` ✗ ICE convergence stalled well below the known-achievable ceiling (92/192, expected ~125)
