# ops 6602 - the 10-year on chart.html (read-only probe)

**Status:** failure  
**Duration:** 61.5s  
**Finished:** 2026-10-21T17:59:28+00:00  

## Error

```
SystemExit: 1
```

## Data

| newest_bar | stat | symbol | today | verdict |
|---|---|---|---|---|
| 2026-10-19 | · vol-cover 0% | TVC:US10Y | 2026-10-21 | STALE |
| 2026-10-19 | · vol-cover 0% | FRED:DGS10 | 2026-10-21 | STALE |
| 2026-10-19 | · vol-cover 0% | US10Y | 2026-10-21 | STALE |

## Log
## 1. the three sources through the worker

- `17:58:27` yahoo live ^TNX  last 2026-10-21 = 5.109000205993652 | n 5 | source yahoo | declared last None
- `17:58:27` ustpar:10Y       last 2026-10-20 = 5.12 | n 9206 | source warehouse:treasury-par (home.treasury.gov daily_treasury_yield_curve) | declared last 2026-10-20
- `17:58:28` FRED:DGS10       last 2026-10-19 = 5.15 | n 16184 | source warehouse:fred-scoped/Interest_Rates (+4 obs tail from FRED, bank healed) | declared last 2026-10-19
## 2. the served chart

- `17:58:29` served chart.html references None; that engine carries the US-yield route change: True
- `17:58:29` installing playwright
- `17:58:54` [TVC:US10Y] newest bar date 2026-10-19 (today 2026-10-21) -> STALE | quote 'TVC:US10Y · 5.15 · unit % · 2026-10-19 UTC display coordinate · source periods 2026-10-19 · last scalar from 2026-10-19 · source frequency D · market OHLC, trade volume and release time unavailable' | stat '· vol-cover 0%'
- `17:59:08` [FRED:DGS10] newest bar date 2026-10-19 (today 2026-10-21) -> STALE | quote 'FRED:DGS10 · 5.15 · unit % · 2026-10-19 UTC display coordinate · source periods 2026-10-19 · last scalar from 2026-10-19 · source frequency D · market OHLC, trade volume and release time unavailable' | stat '· vol-cover 0%'
- `17:59:28` [US10Y] newest bar date 2026-10-19 (today 2026-10-21) -> STALE | quote 'US10Y · 5.15 · unit % · 2026-10-19 UTC display coordinate · source periods 2026-10-19 · last scalar from 2026-10-19 · source frequency D · market OHLC, trade volume and release time unavailable' | stat '· vol-cover 0%'
- `17:59:28` source last dates: {"yahoo live ^TNX": "2026-10-21", "ustpar:10Y": "2026-10-20", "FRED:DGS10": "2026-10-19"}
- `17:59:28` ✗ the chart still shows a stale 10-year for at least one route
