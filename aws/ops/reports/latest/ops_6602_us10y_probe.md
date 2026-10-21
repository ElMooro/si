# ops 6602 - the 10-year on chart.html (read-only probe)

**Status:** success  
**Duration:** 93.5s  
**Finished:** 2026-10-21T19:10:17+00:00  

## Data

| newest_bar | stat | symbol | today | verdict |
|---|---|---|---|---|
| 2026-10-21 | v12.41 · 14217 bars · Vol 0 · 0 prints · yahoo · vol-cover 0% | TVC:US10Y | 2026-10-21 | OK |
| 2026-10-21 | v12.41 · 14217 bars · Vol 0 · 0 prints · yahoo · vol-cover 0% | US10Y | 2026-10-21 | OK |
| 2026-10-20 | · vol-cover 0% | TVC:US02Y | 2026-10-21 | reported |
| 2026-10-19 | · vol-cover 0% | FRED:DGS10 | 2026-10-21 | reported |

## Log
## 1. the three sources through the worker

- `19:08:44` yahoo live ^TNX  last 2026-10-21 = 5.130000114440918 | n 5 | source yahoo | declared last None
- `19:08:46` ustpar:10Y       last 2026-10-20 = 5.12 | n 9206 | source warehouse:treasury-par (home.treasury.gov daily_treasury_yield_curve) | declared last 2026-10-20
- `19:08:48` FRED:DGS10       last 2026-10-19 = 5.15 | n 16184 | source warehouse:fred-scoped/Interest_Rates (+4 obs tail from FRED, bank healed) | declared last 2026-10-19
## 2. the served chart

- `19:08:49` served chart.html references None; that engine carries the US-yield route change: True
- `19:08:49` installing playwright
- `19:09:27` [TVC:US10Y] newest bar date 2026-10-21 (today 2026-10-21, expected 2026-10-21) -> OK | quote '▾ TVC:US10Y 5.13 +0.010000 (0.20%) 1d · price O 5.10 H 5.13 L 5.08 C 5.13 Vol 0 · 2026-10-21T00:00:00Z RVOL Unavailable YTD VWAP 4.27 +20.15% CLV × Vol 0 above YTD VWAP · — ADR 0.0840 · used 58% vs SPX 1d -0.16% · YTD +8' | stat 'v12.41 · 14217 bars · Vol 0 · 0 prints · yahoo · vol-cover 0%'
- `19:09:48` [US10Y] newest bar date 2026-10-21 (today 2026-10-21, expected 2026-10-21) -> OK | quote '▾ US10Y 5.13 +0.010000 (0.20%) 1d · price O 5.10 H 5.13 L 5.08 C 5.13 Vol 0 · 2026-10-21T00:00:00Z RVOL Unavailable YTD VWAP 4.27 +20.15% CLV × Vol 0 above YTD VWAP · — ADR 0.0840 · used 58% vs SPX 1d -0.16% · YTD +8.93%' | stat 'v12.41 · 14217 bars · Vol 0 · 0 prints · yahoo · vol-cover 0%'
- `19:10:01` [TVC:US02Y] newest bar date 2026-10-20 (today 2026-10-21, expected report only) -> OK | quote 'TVC:US02Y · 4.79 · unit Percent · 2026-10-20 UTC display coordinate · source periods 2026-10-20 · last scalar from 2026-10-20 · source frequency D · market OHLC, trade volume and release time unavailable' | stat '· vol-cover 0%'
- `19:10:17` [FRED:DGS10] newest bar date 2026-10-19 (today 2026-10-21, expected report only) -> OK | quote 'FRED:DGS10 · 5.15 · unit % · 2026-10-19 UTC display coordinate · source periods 2026-10-19 · last scalar from 2026-10-19 · source frequency D · market OHLC, trade volume and release time unavailable' | stat '· vol-cover 0%'
- `19:10:17` source last dates: {"yahoo live ^TNX": "2026-10-21", "ustpar:10Y": "2026-10-20", "FRED:DGS10": "2026-10-19"}
- `19:10:17` ✅ the 10-year chart shows today's session on every route
