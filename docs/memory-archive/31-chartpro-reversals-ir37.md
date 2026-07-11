# 31 — Chart Pro terminal, reversal boards, IR v3.5→v3.7 (2026-07-10/11)

## Chart Pro (chart-pro.html) — institutional terminal arc
- Base already had lightweight-charts 4.2 + worker /ohlc any-interval + /quotes + /tv-search + multi-watchlists + RSI/MACD/BB/heat. The delta = FLEET FUSION (JHF class, appended module): Wyckoff phase markers + range lines + badge, insider/MA-event markers via buildMarkers tail (every merge path inherits), auto S/R pivot clusters, AVWAP (auto @ phase begin + click anchors, localStorage jh_avwap_{t}), watchlist engine badges via sigDots prepend, ⚡FUSION/S-R/AVWAP toggles.
- Terminal V2: visible-range VOLUME PROFILE canvas overlay (POC + 70% VA, redraw on pan/zoom/resize via priceToCoordinate), LOG scale, MEASURE two-click ruler, PNG export (takeScreenshot + watermark), RS-vs-SPY hidden-scale overlay (SPY cached per TF), on-chart stats strip (52w off-high/ATR14/RVOL + fusion chips) + 52w H/L lines. Per-pane handles: State._charts[paneIdx]={chart,series,bars,ticker}.
- Δ% Multi-ROC pane: WoW/MoM/QoQ/YoY lines, span-aware lookbacks (day 5/21/63/252, wk 1/4/13/52, mo 1/3/12), zero line, fetch auto-widens +~1y, data-ind="roc".
- Search: jhChartable() gates BOTH surfaces (US stocks/ETFs, JH_INDEX_MAP indices, 6-char forex); ★ favorites (jh_metric_favs, float first, modal manages); empty-focus suggest = Favorites + Recents (knownSymbols); +/−/⌂ TF stepper + hotkeys +/-/0.
- FULL HISTORY: Polygon plan ~5y wall → MAX routes Yahoo; **Yahoo range=max IGNORES interval (~quarterly 168 bars)** → worker translates max → period1=0&period2=now (AAPL 2,380 wk bars to 1980; 11,485 daily). yf-ohlc gained interval param (1d/1wk/1mo, cache-keyed). Universal Polygon→Yahoo fallback (dots→dashes; INDEX_MAP ^-mapping).
- Watchlist + right rail: hidden-by-default slide-overs, persisted (jh_wl_open / jh_rail_open); root causes were forced setLeft(true) and wide-screen auto-open in jh-right-rail.js.
- Conviction drawer removed; Alerts button relocated to toolbar; renderSetups no-op.

## Accumulation reversal boards (accumulation.html, engine v1.4.x)
- v1.4.0/1: REVERSAL TRANSITIONS — context gates (near 252d low/high in 60s) + DATED trigger ≤12–15 sessions (50DMA reclaim/loss, 200DMA break = Weinstein Stage 2/4, 3-mo breakout/breakdown, GC/DC) → boards ⬆bottoms / ⬇tops; CONFIRMED tier = trigger on ≥1.5× 50d vol (O'Neil); U/D vol tape, OBV divergences, distribution-day clusters (IBD ≥5/25), capitulation, churn; on-page methodology + volume guide (Wyckoff/Weinstein/O'Neil/Granville). buf key = "tickers" (not "t"); MAXDAYS 200→235 for cross scans.
- Parallel session v1.4.2: universe-wide 📏 200DMA BREAKS section (no gates, up/down, sessions-ago, vol-confirmed, still-holding flag).

## Industry rotation v3.5→v3.7 (industry-rotation.html)
- Page: QUADRANT CARDS primary (per-quadrant playbook, strength-sorted chips + heading arrows from trail vector) + ROTATION TAPE; ROTATION BARCODE (quadrant colors × ~12wk per ETF); scatter demoted behind details. Ladder: wrap 1280→1680px, sticky sortable headers (data-sort etf/sharpe/scorecard/score/rank_delta_20d — escape quotes inside JS string literals!), zebra/hover, 74vh ladwrap, lad-filter box; row builder extracted buildLadderRows(rowsIn)+wireLadder().
- v3.5: ADAPTIVE rank delta (3+ sessions, rank_delta_days horizon, converges 21); leaders' soldiers fleet chips (phase/whale_musd/er_plus) — mutation via shared holds refs works.
- v3.6: above_sma20 + 20D cross events; soldier ACC/DIST (accumulation-radar sets) + DP; R:R legs via FMP quotes (up to 52wHigh vs stop=priceAvg50→200→52wLow, ratio-colored). **FMP /stable/quote is SINGLE-symbol only** (comma batch silently empty — parallel session probe); serial singles 40ms. ladder_row's array is `closes`, not `c`.
- v3.7: Cross Board columns WYCK/BB(%B+SQUEEZE tightest-quintile 130d)/RSI14/MACD(hist+cross≤5)/EXTREMES(5d/21d/252d ±0.75%); soldiers PE + fwd P/E + FORWARD PEG (fPE÷est EPS CAGR, <1/1–1.5/>2 bands, weaknesses tooltip); on-page R:R legend.
- v3.8/v3.9 (parallel session, TSM catch): **/stable/quote has NO pe/eps fields** (verbatim payload probe) → trailing PE from /stable/ratios-ttm priceToEarningsRatioTTM; FMP ADR estimates arrive in LOCAL currency → scale-sanity gate (fwd/trailing outside 0.2–5×) repairs via NORMALIZED fwd EPS = trailing USD × (1+growth) since the growth RATIO is currency-invariant; forward EPS must be the NEAREST FUTURE fiscal year (one-FY-too-far made NVDA 16.7 vs true 20–24); bounds fwd_pe [3,150], PEG [0.1,10], basis flagged. Ops asserts EXTERNAL-TRUTH BANDS (TSM trailing 20–60 + fwd 15–45; NVDA 24–45 + 15–30) — the band-assert pattern is the new valuation-verify doctrine.

## Ops doctrine additions (hard-won)
- Freshness anchor = module-level T_START (process start), never gate-call time.
- Page-liveness gate must poll a THIS-PUSH marker (old-marker gates grab stale CDN copies).
- Assert every replace (silent-miss zombies); triple-quoted replacements ending in a quote EAT it.
- CF edge can hold plain-URL HTML minutes after ?cb= shows fresh; auditor crawls warm caches (ofr lesson). Browser-style verify = plain URL, no no-cache.
- Failed-marker strings: match method signatures exactly (static async screenshot).
- fmp /stable analyst-estimates: rows epsAvg|estimatedEpsAvg, sort by date, [0]=next FY.
- Yahoo v8: range=max→period1=0 for interval fidelity.
- Parallel sessions active: preflight collisions (3043, 3079) — pull, inspect, renumber.
