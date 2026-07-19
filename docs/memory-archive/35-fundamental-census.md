# 35 — Fundamental Census (S&P-wide sweep + Metric Explorer)
*(ops 3527–3534, 2026-07-19 · engine justhodl-fundamental-census v1.2.0 · page fundamental-census.html pinned Research & Tools · Scheduler fundamental-census-sched cron(0 6 1,15 * ? *) input {"phase":"warm","cursor":0,"refresh":true})*

## What it is
Institutional census of the FULL forensic universe (496 S&P names) through the fundamental-graphs verdict machinery. Extend-don't-rebuild: the census ORCHESTRATES, fundgraph computes.

**Feeds:** `data/fundamental-census.json` (boards) · `data/fundamental-census-matrix.json` (columnar latest-value matrix, 191 metrics × 496 tickers) · `data/fundamental-census-history.json` (60-row daily summary).

## Architecture (v1.2.0, the DURABLE shape)
- phase "warm": **SYNC** invoke fundgraph {"warm": batch, "periods":["quarter"], refresh} with **BATCH=8**, per-link status prints, next link fired in **finally** (Event self-invoke). ~62 links ≈ 2h full refresh.
- phase "aggregate" (optionally {"settle_s":240} first): reads every `data/fundgraph/cache/{SYM}_quarter_v21.json`, extract() → boards + matrix.
- Scoring: **quality = 2×elites + greens − Σsev(reds)**, FUNDAMENTAL basis only (verdict entries with basis=='tech' excluded). Verdict doc shape: `verdicts.greens/reds` lists of `{k, side, sev, label, basis, elite}` — NOT items/color.
- Careful flags: `share_count_yoy_pct` ≥8 → DILUTION_SEVERE (w3), ≥4 → HEAVY (w2); factor_dna goodness pct <10 on beneish/sloan → EARNINGS_INTEGRITY_LOW/ACCRUALS_HIGH (w2); concern <5 → HIGH_CONCERN (w2); each sev-3 red ×3. flag_w sorts the board.
- Matrix: per-doc latest-value map built INSIDE extract() as `_lv` (keys with ≥50% coverage, cap 240; exclude prefixes px_/rsi_/vol/est_). Columnar `cols[k]` aligned to `tickers`; nulls preserved.

## Page + Metric Explorer (OPS3529 block)
Boards (top/worst/careful/10 metric leader-laggard cells/sectors/coverage) + **Explorer**: any-metric dropdown, unlimited chips, **Σ of cross-sectional percentiles** (midrank, ties averaged; nulls excluded per-name from n), per-chip ↑H/↓L (LOW auto via regex debt|days|share_count|pe_|ps_|peg|ev_to|payout|dso|dpo|capex_to|interest_to|sloan|beneish|concern), asc/desc, filter, **dblclick → FGChart modal** (doc via FB function URL ?symbol&period=quarter; series list [{label,u,color,pts,grp:FGChart.grp(u)}], price as own:true; add-metric from doc.points∩FG_CAT; 5Y/10Y/MAX; hand-off link to full comparator). FG_CAT rows are ARRAYS [k,label,tab,unit,flag]. Harness `/home/claude/census2.js` = 10 behaviors exit-enforced.

## Live truth (2026-07-19 full universe)
Top: APP 39(14⭐) · DECK 36 · FIX 34 · NEM 34(12⭐) · ADBE 32 · INCY 32 · ANET 31 · FICO 31 · NVDA 31(13⭐) · EXPE 29. Careful: **SNPS 17 (DILUTION_SEVERE+HIGH_CONCERN)** · AES 15 · BA 15 · BG 15 · EVRG · NCLH · SMCI · WY · AMCR · DOW. Issuer wall COF +62.3%/yr; buyback kings CRM −10.2 / FTV −9.2 / JCI −7.3. avg 9.2 · 74+ flagged. Matrix↔doc exactness proven: AAPL gross_margin 47.862 == 47.862.

## Gotchas (blood-earned, ops 3527–3534)
25. **Aggregate must NOT retain full points**: keeping `_P` per doc ⇒ ~1.5GB @496 ⇒ Lambda SIGKILL ⇒ botocore ConnectionClosed. Extract the latest-value map immediately; drop the doc.
26. **Never sync-wait a 900s Lambda from a 900s Lambda** — the orchestrator dies mid-wait and the chain link never fires. And **Event-invoked heavy batches (25 names) were silently dropped** (zero docs). The proven shape: SYNC small batches (≤8) + finally-guaranteed chaining.
27. **The matrix is written only by the final aggregate** — polling it as a chain-progress odometer reads flat. Progress = v21 cache-doc count (list_objects paginate).
28. **forensic-screen rows drifted ticker→symbol** (2026-07-19 run: ticker null, symbol set). Always parse `r.get("ticker") or r.get("symbol")`; ops-side parsers too, not just engines.
29. Warm invoke response MUST be checked (StatusCode/FunctionError) — 3527's silent pilot failure cost two ops.
30. FB function URL doc contract: GET ?symbol=X&period=quarter → full doc (points/price/verdicts/factor_dna); MSFT 163q proven from the runner.

## Reuse pointers
- Full-history lineage: archive 34 (FETCH/MAX clamps, price stitcher, stmt_rows).
- Sweep-anything pattern: this chain shape (sync-8 + finally) is the template for any future full-universe orchestration.
