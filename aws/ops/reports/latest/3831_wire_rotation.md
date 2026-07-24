# ops 3831 — wire rotation tilt into setups + ranker

**Status:** success  
**Duration:** 18.6s  
**Finished:** 2026-07-24T22:22:22+00:00  

## Data

| best-setups_tilted | master-ranker_tilted |
|---|---|
| 44/50 | 1/25 |

## Log
## G0. rotation feed is fresh and carries what we consume

- `22:22:03` ✅   11/11 sector ETFs, regime=STAGFLATION
- `22:22:03` ✅   every sector ETF carries rrg.quadrant + trend_gate.eligible
## ── justhodl-best-setups

- `22:22:04` ✅   justhodl-best-setups ZIP-SETTLED with '_rotation_scalar' after 0s
- `22:22:13` ✅   invoked clean
- `22:22:13`   rows=50 · field present=50 · actually tilted=44
- `22:22:13`     TSM    x1.05 rotation LEADING via XLK
- `22:22:13`     REGN   x1.05 rotation LEADING via XLV
- `22:22:13`     MU     x1.05 rotation LEADING via XLK
- `22:22:13`     EXE    x0.96 rotation WEAKENING via XLE
- `22:22:13`     DLTR   x0.93 rotation LAGGING via XLP
- `22:22:13`     GILD   x1.05 rotation LEADING via XLV
- `22:22:13` ✅   NON-ZERO JOIN: 44/50 rows tilted
## ── justhodl-master-ranker

- `22:22:14` ✅   justhodl-master-ranker ZIP-SETTLED with '_rotation_overlay' after 0s
- `22:22:21` ✅   invoked clean
- `22:22:22`   rows=25 · field present=25 · actually tilted=1
- `22:22:22`     ORCL   x1.05 rotation tailwind: LEADING via XLK
- `22:22:22` ✅   NON-ZERO JOIN: 1/25 rows tilted
- `22:22:22` ✅ PASS_ALL — rotation tilt live in both rankers
