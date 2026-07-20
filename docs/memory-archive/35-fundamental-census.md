# 35 — Fundamental Census (S&P-wide sweep) · ops 3527–3534 (2026-07-19)

**Engine `justhodl-fundamental-census` v1.2.0** — biweekly (1st+15th 06:00 UTC, Scheduler `fundamental-census-sched`, input `{"phase":"warm","cursor":0,"refresh":true}`). Orchestrates the EXISTING fundamental-graphs verdict machinery (extend-don't-rebuild): phase `warm` sync-invokes fundgraph `{"warm": batch8, "periods":["quarter"], refresh}` per link, `finally`-guaranteed self-chain (Event) to the next cursor; last link chains `{"phase":"aggregate","settle_s":240}`. Aggregate reads `data/fundgraph/cache/{SYM}_quarter_v21.json` for the forensic universe → writes:
- `data/fundamental-census.json` — top_quality/bottom_quality (50 ea), careful (60), metric_boards (10 core, direction-aware best/worst 10), sectors, coverage (honest dormant list), summary; history-lite in `data/fundamental-census-history.json`.
- `data/fundamental-census-matrix.json` — columnar latest-value matrix: every fundamentals metric in ≥50% of docs (tech/px_/rsi_/vol*/est_ excluded), 496 tickers × 191 metrics, aligned `cols{k:[v|null]}` + sectors. **Exactness proven: AAPL gross_margin matrix==doc 47.862.**

**Scoring:** quality = 2×n_elite + n_green − Σsev(reds), FUNDAMENTAL basis only (`basis!='tech'`); verdict doc shape is `{greens:[],reds:[]}` rows `{k,side,sev,label,basis,elite}` (NOT items/color). Careful flags: share_count_yoy ≥8→DILUTION_SEVERE(3)/≥4→HEAVY(2); factor_dna goodness pct<10 → EARNINGS_INTEGRITY_LOW/ACCRUALS_HIGH(2 ea); concern<5→HIGH_CONCERN(2); sev-3 reds ×3. flag_w sorts the board.

**Page `fundamental-census.html`** (FORCE-pinned Research & Tools): hero tiles, top/worst tables, careful board (flashing SEVERE chip), 10 metric leader/laggard cells, sector strip, coverage box. **Metric Explorer (ops 3529–30):** dropdown over matrix metrics, unlimited chips, Σ = sum of cross-sectional percentiles (avg-rank ties, nulls excluded per-name), per-chip ↑H/↓L (LOW auto via regex debt|days|share_count|pe_|ps_|peg|ev_to|payout|dso|dpo|capex_to|interest_to|sloan|beneish|concern), asc/desc, filter; **dblclick → FGChart modal**: `FGChart.render(svg,tip,list,{mode:'val'})`, list rows `{label,u,color,pts,grp:FGChart.grp(u),own?}`, doc via FB URL `?symbol=T&period=quarter`, FG_CAT rows are ARRAYS `[k,label,tab,unit,flag]`, add-metric from doc.points∩catalog, price own-scale, 5/10/MAX, hand-off link `?s=&m=&r=&px=1`. Harness `/home/claude/census2.js` — 10 behaviors, exit-enforced.

**Live full-universe (2026-07-19):** top10 APP 39(14⭐)·DECK 36·FIX 34·NEM 34(12⭐)·ADBE 32·INCY 32·ANET 31·FICO 31·NVDA 31(13⭐)·EXPE 29. Careful: **SNPS 17 (SEVERE+HIGH_CONCERN)**, AES 15, BA 15, BG 15, EVRG, NCLH, SMCI, WY, AMCR, DOW. Issuer wall COF +62.3%/yr; buybacks CRM −10.2/FTV −9.2/JCI −7.3. Avg 9.2 · 74+ flagged.

## Gotchas (25–33, fleet-grade)
25. **OOM by retention:** aggregate kept full `_P` per doc → ~1.5GB @496 → SIGKILL → `ConnectionClosedError` on invoke. Extract latest-values (`_lv`) in `extract()`; never carry points maps across an aggregate.
26. **Sync-wait chain kill:** orchestrator sync-waiting a callee with EQUAL 900s timeout dies mid-wait → self-chain never fires. Either callee-timeout < caller budget or go small-sync.
27. **Event-invoked heavy batches dropped silently** (25-name fundgraph warms produced ZERO docs, no error surface). Fire-and-forget is not delivery. Durable = SYNC small batches (8) + per-link status print + `finally` chain.
28. **Progress odometer:** matrix/census docs update ONLY at aggregate — chain progress must be measured by CACHE COUNT (`*_quarter_v21.json`), never the output feed (3531-E2 false-flat).
29. **Forensic schema drift:** rows moved `ticker`→`symbol` (ticker null) mid-arc. ALWAYS parse `r.get("ticker") or r.get("symbol")`; a zero-universe read is a parse bug until proven otherwise.
30. **Ops-fixture drift:** gate fixtures must drive the REAL extractor (`extract()`), not hand-built internal rows — 3530-D1 tested a shape the engine no longer read.
31. Explorer Σ math: percentile with avg-rank ties `100*((i+j)/2+0.5)/n`; names missing a metric contribute nothing (n-scaled) — EEE-null ordering surprises hand-math.
32. FB modal fetch is the Function URL (CF proxy is data/ only); cache docs per ticker client-side (`DOCC`).
33. Runner brute-force completion: 300 names / 6-batch sync = 50 links, 1,456s, 0 errors — the reliable big-sweep pattern when a chain must finish TODAY.

## Quant-floor arc addendum (ops 3542–3547) + gotchas 34–39
Engine v1.7.2: 13F wb/ws/b/s/n → whale/inst $M cols; finviz-universe short_float_pct + insider_own/trans (496/496); dark-pool board → retail_dp_pct / retail_accum(+1/−1) / retail_dp_score (27 names; ACC 25 / DIST 2 — KKR & ARES the distributions); tech kernel (tech_series/detect_double/beta_vs) — detector v2 = neckline confirmation + extremeness (12% of 78w range) + gap≥6/recent≤12 → live 9 tops/13 bottoms; tech/combo/conviction composites; justhodl-screen-backtest Function URL (hindsight EW basket vs SPX, config feed data/config-backtest-url.json). Harness census2.js = 34 behaviors.
34. Backslash-continuation lines in match anchors silently no-op patches (×3: 3540/3544). Anchor small or regex-DOTALL; every patch prints its own applied-proof.
35. Ops files are ALWAYS written whole — deriving from HEAD/pruned paths made 0-byte files that "ran green" twice (3540, 3545). `wc -c` before push.
36. Read verdicts from aws/ops/reports/<n>.json + latest/<name>.md — no-op runs clobber _lastrun.log.
37. Deploy truth = the DEPLOYED ZIP: download Code.Location, grep the change marker. Descriptions lie (3544 shipped old code under a v1.7.2 label).
38. Beta fixtures need return VARIANCE (constant-growth series ⇒ beta garbage); pattern fixtures need monotonic real dates and ≥6-bar extremum gaps.
39. joins() columns land AFTER the ≥50%-coverage filter (keys=sorted(cols) at the end) — sparse-but-real columns (retail 27, whales 460, earnings 408) ride matrix legally.

## TV-parity arc (ops 3561–3570) + gotchas 40–42
Graphs v1.11.0: +30 raw FMP statement pass-throughs, +44 Statistics metrics (P/B·P/S·P/CF·P/TBV, BVPS/tangible, FCF/sh, Graham, ROA/ROCE/ROTA/ROTE, quick/turnovers, debt suite, eff-int-rate, days inv/payable, payout, gross buyback yield, TCE, 6× per-employee, Zmijewski/Springate/Fulmer) + 5 NTM forwards from analyst estimates. Census floor 0.25/cap 300; matrix 231→293 metrics; categorized optgroups on all 3 explorers (13 fund / 4 ETF-FI). Final: raw 30/30 & stats 43/44 at ≥400 names (inventory_turnover 389 = no-inventory sectors, honest), pe_fwd 482 (SNDK 3.9 cheapest), WMT days-inv 41.97/turn 8.70, MSFT fwd 19.96. NOT-IN-SOURCE (never synthesized): PP&E/acc-dep by class, inventory splits, dom/foreign tax, interest capitalized, notes payable, accrued payroll, dividends payable, separate impairment lines, free float.
40. Substring scans on cache docs LIE — docs echo metric NAMES without series. Data-aware pattern `b'"key": [['` only.
41. run-ops.yml timeout-minutes=30 + concurrency: every ops self-budgets ≤25 min (budgeted while-loop + finally prints); long sweeps split.
42. Parallel-session runs clobber latest/*.md and collide ops numbers — reports/<n>.json (written by the script itself) is the verdict-of-record; check pending/ for foreign files before pushing.
