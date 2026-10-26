# Bank fusion — the banked data inside the engines (2026-10-26)

Khalid, 2026-10-25: "now fuse the new data in all the engines that need them to improve and enrichen those engines."

The provider banks (`docs/providers/WAREHOUSE_BANKS.md`) made the warehouse hold whole-market files the fleet never
had. This program puts them to work inside the engines. It has three layers, built so that an engine integration is a
few reviewed lines, not a re-implementation of the join in 300 places.

## 1. The factor book — `justhodl-bank-factors` (daily 02:50 UTC)

One engine pays the join once a day and writes **one small document per US-listed symbol** plus **one whole-market
matrix** under `data/warm/factor-book/`:

| What | Where | Source bank |
|---|---|---|
| identity (exchange, sector, industry, market cap, flags: ETF / OTC / ADR / fund, IPO date, CIK) | `symbols/<S>.json.gz` → `identity`, `profile` | FMP `profile-bulk` |
| Altman Z, Piotroski | `scores` | FMP `scores-bulk` |
| FMP rating + component scores | `rating` | FMP `rating-bulk` |
| DCF and `dcf_upside_pct` | `dcf`, `derived` | FMP `dcf-bulk` |
| analyst targets (last month / quarter / year averages, counts) and `pt_upside_pct` | `price_target`, `derived` | FMP `price-target-summary-bulk` |
| strong buy / buy / hold / sell / strong sell and `consensus_score` | `consensus`, `derived` | FMP `upgrades-downgrades-consensus-bulk` |
| ~60 TTM key metrics (EV/EBITDA, ROIC, FCF yield, net debt/EBITDA, DSO/DIO/DPO, Graham, NCAV …) | `key_metrics_ttm` | FMP `key-metrics-ttm-bulk` |
| ~60 TTM ratios (PE, PB, PS, P/FCF, margins, coverage, payout, dividend yield …) | `ratios_ttm` | FMP `ratios-ttm-bulk` |
| latest fiscal-year growth (revenue, EPS, net income, FCF, buybacks, share count) | `growth`, `cf_growth` | FMP `income-statement-growth-bulk`, `cash-flow-statement-growth-bulk` |
| earnings surprises → `beat_rate_4q`, `avg_surprise_pct_4q`, `last_surprise_pct` | `surprises`, `derived` | FMP `earnings-surprises-bulk` |
| which ETFs hold the name, at what weight (`etf_holder_count`, `etf_weight_sum`) | `etf_holders` | FMP `etf-holder-bulk` (inverted) |
| peers | `peers` | FMP `peers-bulk` |
| float, outstanding shares, free float % | `float` | FMP walker `shares-float-all` |
| short interest, days to cover (latest settlement + previous) → `short_pct_float` | `short`, `derived` | Polygon bank `short-interest` |
| short-volume ratio (5 sessions) | `short_volume`, `derived` | Polygon bank `short-volume` |
| next earnings date / next ex-dividend date (current month) | `calendar`, `derived` | FMP walker calendars |
| close, returns 1/5/21/63/126/252 sessions, 52-week high/low, `from_52w_high_pct`, 20-day dollar volume, realised vol | `returns` | Polygon tape (`polygon-full` grouped sessions) |
| percentile ranks across US common stocks (`pct_*`) and within the sector (`spct_*`) for 32 fields | `ranks` | computed |
| composites `value`, `quality`, `growth`, `momentum`, `squeeze`, `street` (means of percentiles, lower-is-better inverted, ≥ 2 inputs) | `composites` | computed |

Every document carries `as_of` per source and the bank's own column names; `manifest.json` → `schema_found` says
which column answered each curated field today (a `null` is a header the book did not recognise — it is fixed by
adding the name, never by guessing a value). ETFs, mutual funds, OTC names and preferred shares (`is_preferred`: a
preferred carries its issuer's fundamentals under its own ticker) are documented but **never ranked** and never in a
`screen()`. The matrix is also written one file per column (`matrix/cols/<field>.json.gz` + `matrix/index.json.gz`),
so a consumer asking for five fields reads five small columns, not the 37 MB whole (projection pushdown in the SDK).

First join (ops 6658, 2026-10-26 04:23 UTC, 303 s, 77 MB): 27,215 US-listed symbols — 6,880 common stocks, 5,773 ETFs,
10,248 OTC, the rest funds / preferreds; coverage: scores 16,138, key metrics 16,896, ratios 16,897, growth 14,161,
surprises 8,414, consensus 6,943, targets 4,250, DCF 9,385, ETF holders 9,431 (1.8 M holder rows inverted), float
27,081, short interest 1,434 (polygon-bank still backfilling the per-ticker files), tape returns 12,033; 85 of 86
curated fields resolved on first contact (`rating_score`: FMP's rating-bulk has no overall score column).

## 2. The SDK — `aws/shared/bank_enrich.py`

```python
from bank_enrich import factor, factors, matrix, screen, prescreen, lookup, quote, attach, available, as_of
```
- `factor("AAPL")` → the document (per-container cache); `factors([...])` → threaded; `lookup(sym, fields)` → the
  matrix row (one read per container for thousands of names).
- `screen(where, fields)` / `prescreen(where, order_by, limit)` → whole-market candidates (US common stocks by default).
- `quote(sym)` → a quote-shaped read of the bank close (price, market cap, 52-week high/low, average volume) — labelled
  `source: factor-book (bank close, not a live tick)`; it is for daily engines that need the close, never for an
  intraday desk.
- `attach(out)` → adds `out["bank_enrichment"] = {source, as_of, fields, rows{sym:{…}}, missing}` to an engine's output
  (symbols discovered from the document; additive, the engine's own keys untouched).
- `context(sym)` → the standard labelled `bank_context` block (Altman Z, Piotroski, SI % float, DTC, analyst / DCF upside,
  consensus, 21/63-session returns, 52-week distance, composites) + `as_of` + `source`; `prescreen_rows()` = `prescreen()`
  returning rows; `quote()` now carries `name`.
- `squeeze(sym)` / `squeezes([...])` → the standard short-squeeze block (SI % float, days to cover, float, short-volume
  ratio, market cap, average volume, bank close, 21/63-session returns, distance from the 52-week high, squeeze
  composite) from the projection — the equity-ftd pattern once, for every squeeze / pump / ignition engine (wave 2).
- Never raises, never fabricates (a missing name is in `missing`), every answer carries `as_of`.

Feed readers live next door in `aws/shared/fmp_bank.py` (no key, bank-only): `walker_days(dataset, days)` for a
daily-dated feed (the whole FMP insider feed per day), `walker_latest(dataset, lookback_days)` for the newest banked
page-set of a latest-pages feed (13F latest, senate/house latest, news, M&A), `walker_pages(dataset)` to iterate a
paged reference feed (shares-float-all) one page at a time, `walker(dataset, entity, period=...)` for per-entity files,
`statements(path, symbol, period, limit)` for statement history; `walker_days(..., where=)` / `walker_latest_days(...)` filter on
read.  `polygon_bankfirst.bars(ticker, sessions)` / `closes(ticker, n)` read daily bars straight from the Polygon EOD bank
(complete sessions only by default).

## 3. Engine integrations

Four fusion modes, from the audit (`docs/audit/2026-10-26/bank-fusion-map.md`, `scripts/bank_fusion_audit.py`):
**U** universe — the engine capped its universe for API cost (S&P-only, curated lists, hard caps) → the whole market
from the book; **F** factors — new fields the engine never had; **H** history — full histories where it fetched a few
periods; **E** events/feeds. Rules for every integration: the engine's scoring floors stay unless reviewed; every
new input is labelled (`price_source`, `source`, `bank_context`, `universe_basis`, `book_as_of`); the old path stays
as the fallback when the book is absent (`factor book unavailable` in the output); an offline test covers both paths.

### Wave 1 (2026-10-26)

| Engine | Version | Mode | What changed |
|---|---|---|---|
| `justhodl-magic-formula` | 1.1.0 | U | S&P 500 → S&P 500 + every US common stock ≥ $200M outside Greenblatt's excluded sectors (1,200 candidates pre-ranked by the bank's EY + ROIC; exact EBIT/EV pass unchanged); prices/market caps = bank close (`price_source` per row); no live /quote per name |
| `justhodl-divcut-warning` | 1.1.1 | U + F | 60 curated names → + every US dividend payer ≥ $1B yielding ≥ 3% with a risk marker (payout ≥ 70%, net debt/EBITDA ≥ 4×, yield ≥ 8%), up to 250; the book's names are assessed bank-only (no live retries) inside a 360 s budget (`n_assessed`, `budget_hit`); bank-close quotes; `bank_context` (Altman Z, Piotroski, short % float, analyst upside) on every warning; 1 GB |
| `justhodl-deep-value-screener` | 1.1 | U + F | seed list → whole-market net-cash candidates (cash/share vs price ≥ 25%, NCAV/mcap ≥ 15% or P/S ≤ 0.6; ≥ $200M) first; bank-close quotes + 52-week high from the tape; `bank_context` per row |
| `justhodl-equity-ftd` | 1.1.0 | F | every FTD candidate carries `squeeze` = short interest % float, days to cover, float, short-volume ratio, market cap, 21-day return; the signal rationale quotes them; 20d volume / price fall back to the bank (`vol_source`) |
| `justhodl-stock-valuations` | 1.6.0 | U | the HP small/mid-cap universe ($75M–$3B, > 150k avg volume, NYSE/NASDAQ common) comes from the book; the FMP company-screener (a search endpoint no bank holds) is the second rung; 1.5 GB (its 400-name HP loop touches ~28 quarterly statement year-files through the bank, which must stay resident) |
| `justhodl-analyst-consensus` | 1.1 | U + F | 200 curated names → + every analyst-covered US common stock (≥ 3 targets last month, ≥ $300M, up to 2,500); a name the book holds is scored from the book with zero live calls (`source: factor-book`, `target_basis`); composite weights unchanged |
| `justhodl-quality-on-sale` | 1.1.0 | U | the 5-gate desk stays on its static 50 (its four upstream feeds cover those); a labelled `book_screen` asks the whole market the same question with the bank's facts (quality composite top decile, Piotroski ≥ 7, ROIC ≥ 20%, ≥ 20% off the 52-week high, Altman Z ≥ 3, ≥ $1B) |
| `justhodl-insider-radar` | 1.2.1 | E + F | the first rung of its source ladder is now 35 banked days of the whole FMP insider feed (walker `insider-trading-latest`, per day since 2024) instead of one live page of 1,000 rows; the live and S3 rungs stand. The 60-day decline join reads the desk's rings, then the book's `ret_63d` (Polygon tape) for every other US-listed name, then a capped live Polygon read (40 per run, 60 s) — `ret_60d_basis` counts each; P/S context from the book's `ps` first |
| `justhodl-beneish` | 1.1.0 | U + H | the M-Score across every US common stock ≥ $500M from the bank's statement files (no live call, no pacing); the top-50 `universe_state` keeps its meaning; a `market` block carries the whole-market and per-sector red-flag rates with Altman Z / income quality / SBC / short % float context on each flag |

### Wave 2 (2026-10-26, 18 engines; built by four parallel builders under the wave-1 rules, reviewed, gate-tested)

| Engine | Version | Mode | What changed |
|---|---|---|---|
| `justhodl-pre-pump-detector` | 2.1.0 | F + H | price history from the Polygon EOD bank first (`history_source` per row), FMP second; the standard `squeeze` block on every result; `universe_basis`, `book_as_of`, `factor_book` labels |
| `justhodl-pump-mechanics` | 1.1.0 | F + H | profile from the book (`profile_source`), history bank-first; `squeeze_profile.short_interest` = the REAL SI % float / days to cover / short-volume ratio next to the proxy score (score byte-unchanged; SEC/massive consumer contracts untouched) |
| `justhodl-ignition` | 1.2.0 | F + E | profile pillar from the book; insider pillar from 45 banked feed days instead of one live search per ticker (live rung kept); `squeeze` context per rank |
| `justhodl-coffee-can` | 1.1.0 | F | quotes from the bank close (`price_source`), thesis series from `fmp_bank.statements` (`thesis_detail.sources`), `bank_context` per holding; thesis rules untouched |
| `justhodl-buyback-yield-ranking` | 1.1.0 | U + F | master-ranker list + whole-market buyback candidates (≥ $2B, non-ADR, shrinking share count; cap 600, bank-only); TTM cash flows from the bank's quarterly statements (`cashflow_source`); five gates + weights unchanged |
| `justhodl-eva-spread` | 1.1.0 | F + U | TTM inputs from the book, EVA history from bank statements; new labelled `market` block (every US common ≥ $2B, cap 1,500: spread, regime, sector breakdown, super-compounders) next to the unchanged STATIC_TOP50 `universe_state` |
| `justhodl-smart-beta` | 1.1.0 | F + U | value/quality inputs from the book, 252 closes from the Polygon EOD bank; new `market` block (cap 1,500) with low-vol labelled PROXY (`factor_notes`) |
| `justhodl-dividend-growth` | 1.1.0 | U + F | universe = book dividend payers (0.5–9 %) ∪ screener rows, top 700 by mcap; dividend history from the walker (`history_source`); `bank_context`; the screener failing is no longer fatal |
| `justhodl-pead-detector` | 1.1.0 | F + U | earnings rows walker → book surprises → live (one call instead of two); drift from book returns (`drift_basis`); beat-streak candidates from `prescreen_rows` (cap 600); scoring/tiers untouched |
| `justhodl-post-earnings-mean-rev` | 1.1.0 | F + U | report date walker → book → live; price from the bank close; 80 closes walker → Polygon EOD bank → live (`history_source`); universe = master-ranker 120 + banked earnings-calendar names that reported 5–15 sessions ago (≥ $2B, cap 200) |
| `justhodl-rating-change-cluster` | 1.1.0 | E + F | feed = banked `grades-latest-news` days (deduped, window-bounded; `feed_basis`), live page fallback; picks priced from the book |
| `justhodl-starmine` | 1.1.0 | U + F | static top-50 core + analyst-covered candidates from `prescreen_rows` (pt_n_1q ≥ 5, ≥ $5B, cap 150); prices/grades/targets/surprises bank-first with `basis` per row; pacing applies to live calls only |
| `justhodl-insider-sell-cluster` | 1.1.0 | E + F | 16 banked feed days filtered on read (strict `S-` sale test; 10b5-1 excluded); quotes bank-first, live capped 25; `bank_context` |
| `justhodl-insider-cluster-scanner` | 2.1.0 | E + F | 33 banked feed days of purchases mapped to the Form-4 schema (SEC EDGAR sample + XML parse stands as the fallback); top-50 enrichment from the book (`fundamentals_source`); issuer name from the book (the FMP feed carries none) |
| `justhodl-insider-buyback-confluence` | 1.1.0 | E + F | feeder first, 60 banked feed days fill the names it lacks (`insider_source`); quotes bank-first, live capped 20 |
| `justhodl-smart-money-tracker` | 2.1.0 | E | discovery from the banked 13F latest page-set (200 CIKs vs 80 live); per-filer walker files first, live capped 40 |
| `justhodl-smart-money-holdings` | 1.1.0 | E | per-fund `institutional-ownership-extract` from the walker (4-quarter fallback), live extract only for unbanked funds, capped 20 |
| `justhodl-political-trades` | 1.1.0 | E + F | senate/house latest from the walker first (3,000 rows per chamber at 256 MB), live ladder only for a chamber the bank lacks; `bank_context` on clusters and large trades |

Skipped as reviewed-contract engines (byte-pinned sources / archived originals; each needs its own reviewed change):
`justhodl-momentum-leaders`, `justhodl-pump-positioning`, `justhodl-estimate-revisions` (with `eps-revision-velocity`,
`earnings-quality`, `short-interest` from wave 1).

Wave-2 lessons folded into the platform: (1) a FakeBook that returns rows where the real `prescreen()` returns symbols
passed two engines' tests and would have emptied their book universes in production — `prescreen_rows()` now exists and
`tests/deployment/test_bank_sdk_contracts.py` checks every SDK call in every engine against the real modules' attributes
and signatures; (2) the book's `ret_*` / `from_52w_high_pct` / `short_pct_float` / `pt_upside_pct` are PERCENT, its
`dividend_yield` / `fcf_yield` / `roic` / growth fields are FRACTIONS (FMP's own units) — insider-radar v1.2.1 multiplied
a percent by 100 (fixed in v1.2.2); (3) the whole insider feed is ~3,000 rows a day — stream and filter on read
(`walker_days(where=...)`), never retain 35 raw days (insider-radar v1.2.2, 1 GB); (4) FMP's profile names a preferred /
baby bond after its ISSUER, so symbol + name heuristics missed plain-ticker notes (UZD, OXLCM ranked as 63 %-yield
dividend payers) — book v1.0.5 joins the Polygon reference list's instrument `type` per symbol (`security_type`,
`is_common`, `is_debt_or_structured`) and the SDK's `common_only` honours it.

### Wave 3a (2026-10-26) - the history book, for the on-demand per-ticker desks

The fleet error census since the FMP block (ops 6664; 169 engines invoked, 9 with errors) put one failure far ahead of
the rest: `justhodl-equity-research`, the why.html per-ticker desk - 52 `Runtime.OutOfMemory` in 722 user-driven
invocations, memory pinned at 1,023 MB on every request, and "No data for ticker AAPL" when forced to recompute. Cause:
it asks 20 annual + 22 quarterly years of three statements plus 15 years of ratios / key metrics for ONE ticker; in the
whole-market year-file layout that is ~60 tables (hundreds of MB decoded) per request, so the bank-first shim's table
cache thrashed and it stepped aside, the live FMP call answered 429, and the statement asks were deeper than the shim's
year-file ceiling (12 annual / 16 quarterly) anyway.

`justhodl-bank-histories` (daily 03:05 UTC, 3 GB, 6 GB /tmp) inverts the year-files once a day into per-symbol
documents - `data/warm/history-book/<bulk>/<annual|quarter>/<SYMBOL>.json.gz`, the nine dated bulks (income / balance /
cash-flow statements, their growth files, financial-growth, ratios, key-metrics), annual from 2000, quarterly from 2016,
the factor book's US-listed universe, write-if-changed by hash state, lease-guarded, `manifest.json` with per-block
refresh times (`stale_blocks`, `phase`). `fmp_bank.statements()` reads the book first (one small object, any depth),
the year-file scan stays as the fallback, and the shim accepts a statement ask deeper than its ceiling whenever the book
holds the symbol (`fmp_bank.history_has`). First build is conducted by `.github/workflows/fmp-backfill.yml`
(lease-guarded, counted kicks; a subset dispatch can no longer disable the daily backfill). equity-research moves to
1.5 GB for headroom; its 10-thread first touch is covered by the one-loader-per-table fix.

### Queue (next waves, from the fusion map)

- squeeze / short: pre-pump-detector, pump-mechanics, ignition, short-interest, early-movers, momentum-leaders (SI % float, DTC, float, short-volume ratio).
- quality / value screens: coffee-can, buyback-yield-ranking, eva-spread, smart-beta, fortress, forensic-screen; gf-value needs a memory redesign first (it runs at its 1.5 GB ceiling on the S&P 500 alone).
- earnings: pead-detector, post-earnings-mean-rev, estimate-revisions, rating-change-cluster, starmine (surprises, estimates, grades). `eps-revision-velocity`, `earnings-quality` and `short-interest` run reviewed original-source contracts (acquisition plans, archived originals, "preserve previous publication") - a fusion there is its own reviewed change, not a universe patch (the 2026-10-26 eps-revision-velocity patch touched only its dormant legacy path and was withdrawn).
- insider / 13F / political feeds: insider-cluster-scanner, insider-sell-cluster, insider-aggregate, insider-buyback-confluence, smart-money-*, 13f-*, political-trades (walker feed readers, the insider-radar pattern).
- energy: refining-stress, inventory-drawdown, commodity-curves, metals-miners (an `eia_bank` reader).
- crypto: crypto-scorecard, crypto-etf-arb, crypto-* (CMC history, CryptoQuant hourly).
- ETF: etf-true-flows, etf-census, index-inclusion, russell-recon-frontrun, rebalance-radar (ETF Global flows / constituents, ETF holders).
- byte-ledgered engines (microcap-float-squeeze, volatility-squeeze-hunter, earnings-pead, momentum-breakout …) need a ledgered patch each.

## Verification

- `aws/ops/STAGED/ops_6658_bank_factors_birth.py` — the book's birth: schedule declared, first join, coverage per source,
  `schema_found`, AAPL / NVDA / GME / SPY read back, first whole-market screen.
- Wave-1 first light: `aws/ops/STAGED/ops_6659_bank_fusion_first_light.py` kicks the fused engines once and reads each
  output for the new labels (`universe_basis`, `price_source`, `bank_context`, `squeeze`, `source: factor-book`,
  `book_screen`, `market`) plus the log groups for errors.
- The book's first join (ops 6658, 03:47 UTC) died Runtime.OutOfMemory in 19 s decoding whole-market CSVs into strings;
  v1.0.1 streams every table once into a SQLite staging file on /tmp (benchmark: 25k symbols, 600k ETF-holder rows,
  64 s, 725 MB peak) - the trap for any future whole-market join: never `.decode()` a bulk file, stream it.
- First light (ops 6659 + 6661, 2026-10-26 04:58–05:40 UTC): magic-formula 910 eligible / every price from the bank
  close (185 s, 1.5 GB); analyst-consensus 649 names, 644 scored from the book; quality-on-sale `book_screen` 3 names;
  beneish 2,968 scored, 463 red flags (18 s); deep-value ran but 492/500 of its first whole-market candidates were
  SPAC shells / ADRs with no balance sheet → the pre-screen now requires common, non-ADR, 0 < P/S ≤ 3 (v1.1).
  Three engines failed and were fixed before wave 1 could be called verified:
  - **divcut-warning** and **stock-valuations** died `Runtime.OutOfMemory` (768 MB in 9 s; 640 MB in 56 s) in their
    per-ticker phase. Cause: `aws/shared/fmp_bank._table()` had no per-key in-flight dedup — eight worker threads each
    missed the cache for the same bulk CSV at the same instant and each fetched + decoded it (gz bytes + bytes + str ≈
    3× the file). Fix: one loader per key (waiters reuse the built table) and at most one whole-market decode in flight
    below 1.5 GB (two above); `bank_enrich` document cache 3000 → 500; `quote()` reads the column projection instead of
    a per-symbol document. divcut also assesses the book's names bank-only within a 360 s budget (446 live-retrying
    names → ≤ 250 bank reads); stock-valuations moved to 1.5 GB so its ~28 quarterly year-files stay resident.
  - **insider-radar** timed out 3 × 300 s: 35 banked feed days meant one live Polygon 60-day read per insider ticker
    (hundreds, serial). Fix (v1.2.1): rings → the book's `ret_63d` → live Polygon capped at 40 reads / 60 s, computed
    once per ticker; P/S from the book's `ps`.
  - **equity-ftd** did not refresh: it runs only when a new SEC FTD file lands (expected; its fusion is covered by its
    offline test and the next SEC drop).
  - `eps-revision-velocity`: "All provider populations unavailable; preserve previous publication" is its pre-existing
    reviewed contract, not a fusion failure — the patch there was withdrawn (see queue).
