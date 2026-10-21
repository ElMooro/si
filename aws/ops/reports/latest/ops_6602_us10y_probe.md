# ops 6602 - the 10-year on chart.html (read-only probe)

**Status:** failure  
**Duration:** 80.5s  
**Finished:** 2026-10-21T18:47:51+00:00  

## Error

```
SystemExit: 1
```

## Data

| newest_bar | stat | symbol | today | verdict |
|---|---|---|---|---|
| 2026-10-02 | v12.41 · 16176 bars · Vol 0 · 0 prints · Warehouse OHLC · provider identity and historical completeness unverified · vol | TVC:US10Y | 2026-10-21 | STALE |
| 2026-10-21 | v12.41 · 14217 bars · Vol 0 · 0 prints · yahoo · vol-cover 0% | US10Y | 2026-10-21 | OK |
| None |  | TVC:US02Y | 2026-10-21 | reported |
| 2026-10-19 | · vol-cover 0% | FRED:DGS10 | 2026-10-21 | reported |

## Log
## 1. the three sources through the worker

- `18:46:31` yahoo live ^TNX  last 2026-10-21 = 5.111000061035156 | n 5 | source yahoo | declared last None
- `18:46:33` ustpar:10Y       last 2026-10-20 = 5.12 | n 9206 | source warehouse:treasury-par (home.treasury.gov daily_treasury_yield_curve) | declared last 2026-10-20
- `18:46:33` FRED:DGS10       last 2026-10-19 = 5.15 | n 16184 | source warehouse:fred-scoped/Interest_Rates (+4 obs tail from FRED, bank healed) | declared last 2026-10-19
## 2. the served chart

- `18:46:34` served chart.html references None; that engine carries the US-yield route change: True
- `18:46:34` installing playwright
- `18:47:00` [TVC:US10Y] newest bar date 2026-10-02 (today 2026-10-21, expected 2026-10-21) -> STALE | quote '▾ TVC:US10Y 5.28 +0.0400 (0.76%) 1d · price O 5.22 H 5.30 L 5.16 C 5.28 Vol 0 · 2026-10-02T00:00:00Z RVOL Unavailable YTD VWAP 4.27 +23.66% CLV × Vol 0 above YTD VWAP · — ADR 0.0690 · used 204% vs SPX 1d +0.03% · YTD +12' | stat 'v12.41 · 16176 bars · Vol 0 · 0 prints · Warehouse OHLC · provider identity and historical completeness unverified · vol-cover 0%'
- `18:47:20` [US10Y] newest bar date 2026-10-21 (today 2026-10-21, expected 2026-10-21) -> OK | quote '▾ US10Y 5.11 -0.006000 (-0.12%) 1d · price O 5.10 H 5.12 L 5.08 C 5.11 Vol 0 · 2026-10-21T00:00:00Z RVOL Unavailable YTD VWAP 4.27 +19.77% CLV × Vol 0 above YTD VWAP · — ADR 0.0840 · used 48% vs SPX 1d -0.82% · YTD +8.20' | stat 'v12.41 · 14217 bars · Vol 0 · 0 prints · yahoo · vol-cover 0%'
- `18:47:39` [TVC:US02Y] newest bar date None (today 2026-10-21, expected report only) -> OK | quote 'Observation history unavailable for TVC:US02Y; no other series is substituted' | stat ''
- `18:47:51` [FRED:DGS10] newest bar date 2026-10-19 (today 2026-10-21, expected report only) -> OK | quote 'FRED:DGS10 · 5.15 · unit % · 2026-10-19 UTC display coordinate · source periods 2026-10-19 · last scalar from 2026-10-19 · source frequency D · market OHLC, trade volume and release time unavailable' | stat '· vol-cover 0%'
- `18:47:51` source last dates: {"yahoo live ^TNX": "2026-10-21", "ustpar:10Y": "2026-10-20", "FRED:DGS10": "2026-10-19"}
- `18:47:51` ✗ the chart still shows a stale 10-year for at least one route
