# chart.html restore - 2026-10-21 (ops 6596-6599)

Khalid, 08:36 CT: "bring back chart.html with all the engines inside it".

## What the chart is
`chart.html` (58 KB shell, ~40 `jh-chart-*.js` modules) with a 22-button desk strip: Macro, Heat, Auction, Corr,
ETF, Strong, Bonds, CDS, 13F, SC, Capit, BC, Spread, Spring, Accum, Bottom, Dist, Pump, Khalid, Demand, Supply,
Alert (+ GO). Fifteen desks are iframes of the engine pages (macro-economic-data, universe-heatmap, auctions, etf,
strong, bonds, cds, 13f, wyckoff-desk x6, alerts, bb-go); Corr and Khalid render inline; Accum / Bottom / Dist /
Demand / Supply mark the chart. The page contract binds 82 outputs of 39 engines to it; the bars come from the
data-proxy worker -> `justhodl-symdir` Function URL (`/warehouse-ohlc`) -> `data/warm/tv-bars/universe/*.json.gz`
(banked by `justhodl-tv-bars`).

## Findings (ops 6596 census, ops 6597 headless-Chrome probe of the served page)
| # | finding | cause | fix |
|---|---|---|---|
| 1 | chart blank: quote HUD empty, watchlist "Unavailable", "No matching valid dated chart frame"; 1,790 x 502 on `/ohlc` + `/yf-ohlc`, 429 on `/series` | ops 6558 (2026-10-20) capped the 102 heavy public URL functions at reserved concurrency 2; symdir (6 GB) is one; a page fans out dozens of bar requests | symdir 2 -> 30 (ops 6598; runbook layer-3 rule); re-probe: `AAPL 342.94 -0.52 (-0.15%)`, 11,549 bars, 0 x 502 |
| 2 | 33 of 82 bound outputs STALE (freeze Oct 16-21), 10 writers not run since the thaw | post-close engines fire after 21:00 UTC | one on-demand run each: cryptoquant, dix, gf-value, etf-global-desk, squeeze-pretrigger refreshed 14:26-14:30 UTC; dark-pool refused a same-day re-attempt (tonight's run stands); options-confluence / brief-compiler fire this afternoon |
| 3 | `data/fmp-ratios.json` DEAD (one-off ops 5576 harvest 2026-09-15, 40 names) - Valuation / Financials desks and `fmp_book.try_harvest` (238 engines) served it as current | no writer | new engine `justhodl-fmp-ratios` (daily 22:38 UTC): core 40 + TV-banked US names + S&P 500, ~12 req/s throttle, last-good merge, fail-closed on a bad key; first run 506 names / 0 errors / 2.0 MB; `fmp_book.row('AAPL')` as_of 2026-10-21 |
| 4 | bar bank frozen at 2026-10-19 (288 of 309 names) | TradingView websocket handshake refused (HTTP 400) since ~Oct 12; `justhodl-tv-bars` refuses to fall back on a refusal | Khalid's decision (KHALID_ACTIONS 0k): re-auth TV / Yahoo fallback / Polygon US append |
| 5 | `justhodl-estimate-revisions` 16 errors / 16 runs | Benzinga calendar via Polygon answers unauthorized since Oct 10 | subscription question (Khalid) |
| 6 | `justhodl-share-flows` 7/7 errors, output from Sep 26 | upstream GitHub job `capital-structure-source-refresh.yml` fails at "plan" daily since Oct 8 | other lane's producer; reported |
| 7 | `justhodl-earnings-pead` 3 errors, output Oct 15 | "No selected universe; preserve previous publication" | reported |
| 8 | 1,500 wasted requests per session on dead fallbacks (`/api/klines`, `/api/yahoo`, `/api/trades` 404; `/data/series/*.json` 403); edgar-insiders URL called directly (404) | page code | cleanup proposed |
| 9 | 3 contract outputs MISSING (`symbology/bond-cusips.json`, `symdir/wb-indicators.json.gz`, `tv-descriptions.json`) | never written / renamed | contract hygiene, later |

## Cost of the fix
symdir at 30 concurrent: worst-case flood ~$10.8/hour (6 GB x 1.3 s per call), stopped by the governor's fuse
(>= $20/day burn over its window) and the runaway alarms within ~20 minutes; normal traffic is ~1,600 calls/day
(~$0.2). Five on-demand engine runs: ~$0.05. fmp-ratios: ~7 minutes x 1 GB daily (~$0.01) and ~5,000 FMP calls
inside the plan.

## Watch
* `justhodl-symdir` monthly cost vs the $50 engine cap (CAP hold would blank the chart again): ~$27/month today; if
  the chart gets busier, set `engine_caps["justhodl-symdir"]` in control.json (Khalid's call).
* The anomaly sentinel: a heavy chart session can push symdir past 200 invocations in an hour; its baseline is in
  the hundreds per hour on busy days, so a single session should not strike.
