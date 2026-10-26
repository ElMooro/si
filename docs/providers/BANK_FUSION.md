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
- Never raises, never fabricates (a missing name is in `missing`), every answer carries `as_of`.

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
