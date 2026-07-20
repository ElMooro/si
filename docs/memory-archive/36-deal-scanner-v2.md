# 36 — Deal-Scanner v2.0.0: full-market + graded deal-win family (ops 3571–73, 2026-07-20)

Trigger: IREN $2.8bn AI contract fired on v1 and pumped +19% same day — Khalid ordered
full-market coverage (all industries, all caps) + fusion into every engine that needs it.

## Engine (justhodl-deal-scanner, VERSION 2.0.0, timeout 420s)
- Sources: FMP /stable/news/press-releases-latest + stock-latest 14 pages each (2,800)
  + Polygon news 8 pages (800) = ~3,600 items/run, ~1,300 unique tickers on tape.
  NO universe filter — every ticker on the PR/news tape is eligible.
- Benzinga leg DELETED (Massive 403 fleet-wide since 2026-07-15; key removed from source).
- Origin tagging per item (fmp_pr / fmp_news / polygon) → coverage.sources counts.
- Crossref cap 300→450 tickers (revenue + market cap), 20 workers.
- NEW feed blocks: by_sector (all 11 GICS, zeros shown), by_cap (nano→mega, 6/6 hit day one),
  coverage {sources, n_items, n_unique_tickers_in_tape, sectors_with_deals/11, caps_with_deals/6,
  runs_per_day 8}. deals[:120]→[:200]. summary += signals_logged, signals[], sectors/caps hit.

## Graded family: deal-win (shared signals_emit → regime-stamped, suppress-aware, deduped)
- Bar: age_h ≤ 30 AND (highlight green OR ai_megadeal OR ($1B+ AND vs_mc ≥5%)), ≤10/run.
- UP [5,21,63] vs SPY at announcement-time price (yprice). Conf 0.66 green+AI-mega / 0.62 green / 0.58.
- Metadata: deal_value_usd, vs_mc_pct, materiality_pct (9999=pre-revenue), cap_bucket, sector,
  highlight, ai_megadeal, age_h + regime snapshot (free via log_signal).
- DAY-ONE: IREN ($4bn ARR target, 33.34% mcap, conf .66, base 40.237) · EPR ($1.6B, 33.6%) ·
  MRAI ($26.4M, 75.2% mcap!) · KMDA ($50M, 12.2%). DDB row deal-win#IREN#2026-07-20 verified.
- Grading loop + alpha-triage + PROVEN gate now apply automatically to the family.

## Fusion (ops 3572, all PASS)
- best-setups: _deal_idx from data/deal-scanner.json (≤72h, best-score per symbol) →
  _s["deal_context"] in the setups[:25] enrichment loop (same slot as sector/census context).
- master-ranker: t["deal_win"] overlay (mirrors khalid_note/squeeze_fuel), n_deal in fusion print.
- morning-intelligence: feed "deal_scanner" + facts fresh_deal_wins[:5] + deal_signals_today.
- why.html: #jhDealRadar section + window.fillJHDealRadar (mirrors ops-3299 DollarFlows closure,
  uses gj9/E9 helpers) — up to 3 fresh deals for the ticker, pills, link to full board.
- alpha-families.html: 7th card c-deals (deal-win · [5,21,63] vs SPY), feed appended to
  Promise.allSettled (positional destructure ...,dw), footer feed list updated.
- deal-scanner.html ADDITIVE: +2 statbar tiles (Sectors Hit x/11, Graded Signals),
  Market Coverage sector table, All Cap Tiers table, Graded Signals strip, note rewrite.

## Gotchas (new)
- ⚠️ LIVE EB rule deal-scanner-daily was ALREADY cron(5 */3 * * ? *) (every 3h, 8 runs/day) while
  config.json said daily 22:00 — deployed cadence had been upgraded out-of-band. ALWAYS
  events.describe_rule the live cadence before touching schedules; never downgrade live reality.
  Ops 3573 aligned config + all page/engine strings to every-3h. Parity verified.
- ⚠️ Sandbox: create_file tool writes under /home/claude/... but bash HOME=/root — copy artifacts
  into the /root/work/si clone before git add, or files silently miss the commit.
- coverage.n_tickers_crossref counts DEAL-candidate tickers (post-filter), not tape tickers —
  20 on a normal day is correct; the 450 cap matters on heavy news days.

---

# v3.0.0 — INSTITUTIONAL LAYER + EXHAUSTIVE US-LISTED UNIVERSE (ops 3574–76, 2026-07-20)

Trigger: Khalid — "include all stocks listed in the usa and make it the way institutions and hedge funds would want it."

## universe-builder v4 EXHAUSTIVE (ops 3574 PASS_ALL)
- Was v3: limit=1000/bucket (small/mid TRUNCATED), $5M floor, country=US (EXCLUDED ADRs like TSM/BABA), NO config.json (never CI-deployed).
- v4: adaptive mcap-range **bisection** — when a screener range returns ≥1000 rows (FMP hard cap), split at geometric midpoint `int((low*high)**0.5)` and recurse (depth ≤10) → **zero truncation**. country filter removed (US-listed = NYSE/NASDAQ/AMEX incl. ADRs). Floor $1M. Per-stock adds `exchange/country/is_adr`; stats adds `by_exchange/n_adr`. schema_version 4, method `universe_builder_v4_exhaustive_bisection`. Shape otherwise unchanged — 13+ consumers unaffected.
- **LIVE: 5,315 stocks** (from ~2,400), 1,241 ADRs, 8 venues (NYSE 1,903, NASDAQ-GS 1,130, NASDAQ-CM 769, NASDAQ-GM 742, NASDAQ 538, NYSE-American 189, Arca 43, "NYSE" 1). Buckets: nano 968 / micro 961 / small 1,306 / mid 1,130 / large 880 / mega 70.
- Schedule: existing Schedulers found (`justhodl-screen-builder-universe-daily`, `finviz-universe-sched`) → live cadence kept, nothing created (3573 doctrine).

## deal-scanner v3.0.0 (ops 3575 4/5 + 3576 config fix)
New module helpers: `EVENTS_ALL` (8 types) + `_EV_PATTERNS` regex ladder (**ma_target checked FIRST**) + `classify_event` · `counterparty()` curated `_CP_GOV/_CP_HYPER/_CP_MEGA` → GOV/HYPERSCALER/MEGACAP/NAMED/UNNAMED · `promo_guard` (LOI/MOU/non-binding regex, ≥2 promo phrases, nano/micro+UNNAMED+sizeless pump) · `load_universe_meta` (exch_map from data/universe.json) · `load_census` (best-setups census_idx pattern) · `load_13f_flows` (`tf.t`: b/s/n $, wb/ws/wn whale, nf funds) · `load_8k_set` (SEC EDGAR `efts.sec.gov/LATEST/search-index?q="Item 1.01"&forms=8-K` last 3d, from=0/100/200, UA "JustHodl research contact@justhodl.ai"; CIK→ticker via `sec/company-tickers.json` S3 cache 7d TTL from sec.gov/files/company_tickers.json; returns None on failure — non-fatal) · `_poly_closes` + `pop_since_announce` (close-before-announce vs latest).

Per-deal fields: `event_type, counterparties, counterparty_quality, listed, exchange, promo_risk, non_binding, confirmed_8k, census, inst_flow`. Score `v3_adj`: +25 GOV/HYPERSCALER (+8 MEGACAP), +12 8-K, −60 promo, −40 ma_target, −25 unlisted.

**History ledger** `data/deal-history.json`: entries keyed `sha1(sym|announce|title[:60])[:16]`, prune 120d, SPY closes fetched ONCE per run, `_fwd` computes trading-day 5/21 excess vs SPY, ≤40 fills/run skipping unlisted, `base_rates` per event type `{n5, med_fwd5_ex, n21, med_fwd21_ex, hit21}` — the POPULATION study the graded family is measured against.

**Signal bar v3**: adds `listed is not False` + `event_type != ma_target` + `not promo_risk`; chase-guard `pop_since_announce ≥25%` → skip unless AI-mega+green, else conf −0.06; conf +0.04 GOV/HYPERSCALER, +0.03 confirmed_8k, clamp [0.50, 0.74]. Metadata/logged += event/quality/8k/pop/exchange.

Feed adds `by_event`, `base_rates`, `history`, `coverage.universe{n_listed,n_adr,by_exchange,generated_at}`, `n_8k_item101_3d` (**LIVE: 34** 8-Ks matched). config: mem 1024, timeout 600.

Pages (additive): deal-scanner.html — "US-Listed Monitored" statbar tile, v3 card pills (event badge, 🏛 GOV/HYPERSCALER+name, 📄 8-K ✓, ⚠ PROMO-RISK, OTC/UNLISTED), "Event Mix" board, "Base Rates" table with young-ledger empty state, institutional-layer note. alpha-families c-deals meth line. why.html Deal Radar metric chips (event, 8-K ✓, 🏛 quality, promo).

## Gotchas (this arc)
1. **⚠️⚠️ deploy-lambdas.yml config-stomp**: workflow left deal-scanner at DEFAULT **512MB/300s** despite a valid config.json in the diff — timeout even REGRESSED 420→300 (so update-function-configuration DID run, with defaults → config read failed at runtime somewhere; Actions log blobs unreachable from sandbox egress). **Doctrine: after any workflow deploy, gate MemorySize/Timeout in ops; fix via `update_function_configuration(Memory+Timeout ONLY)`** — env untouched by construction — then settle-poll (ops 3576 pattern, PASS_ALL).
2. Patcher import anchors: file has separate `import json` / `import re` lines, not comma-joined — a failed rep raised BEFORE the write, so the file stayed clean v2 and the patcher was safely re-run in verified blocks.
3. `sh` (bash_tool default) has no process-substitution `<(…)` — use plain python validators.
4. 8-K join is enrichment-only: gates never depend on EDGAR availability (null-tolerated).
