# ops 4712 — re-prioritize ICE-first, drive to completion this session

**Status:** success  
**Duration:** 124.9s  
**Finished:** 2026-08-15T18:18:33+00:00  

## Log
## 1. Live state (not assumed — checking now)

- `18:16:29`   catalog=225 done=94 status=converging
## 2. Re-prioritize: BAML/ICE series first

- `18:16:29`   reordered: 192 BAML first, 33 other after
- `18:16:29`   BAML already done before this run: 92/192
## 3. Drive repeated invocations until ICE converges or time runs out

- `18:18:32`   round 1: {"ok": true, "pulled": 0, "failed": 60, "row_cap_hits": 0, "done": 154, "catalog": 225, "status": "converging", "mean_agree_pct": 100.0}
- `18:18:33`     BAML progress: 152/192
- `18:18:33` ✅   ICE board converged (accounting for the 67 known-absent EM series)
## 4. Final tally

- `18:18:33`   ICE/BAML final: 152/192 done, 60 in failures dict (rounds=1, elapsed=124s)
- `18:18:33`   mean cross-check agreement on ICE series: 100.0% (n=92)
- `18:18:33`     miss: BAMLEM1BRRAAA2ACRPIEY (HTTP 403)
- `18:18:33`     miss: BAMLEM1BRRAAA2ACRPIOAS (HTTP 403)
- `18:18:33`     miss: BAMLEM1BRRAAA2ACRPISYTW (HTTP 403)
- `18:18:33`     miss: BAMLEM1BRRAAA2ACRPITRIV (HTTP 403)
- `18:18:33`     miss: BAMLEM1RAAA2ALCRPIUSEY (HTTP 403)
- `18:18:33`     miss: BAMLEM1RAAA2ALCRPIUSOAS (HTTP 403)
- `18:18:33`     miss: BAMLEM1RAAA2ALCRPIUSSYTW (HTTP 403)
- `18:18:33`     miss: BAMLEM1RAAA2ALCRPIUSTRIV (HTTP 403)
- `18:18:33`     miss: BAMLEM2BRRBBBCRPIOAS (HTTP 403)
- `18:18:33`     miss: BAMLEM2BRRBBBCRPISYTW (HTTP 403)
## verdict

- `18:18:33` ✅ ICE board driven to 152/192 (ceiling ~125 given 67 known-absent EM series) — 100.0% mean cross-check agreement vs FRED
