# The provider banks — pulling 100% of what the keys reach (2026-10-25)

Khalid, 2026-10-25: "fix it all, I want 100% of the data pulled and used by my system."
Audit that started it: `provider-coverage-audit-2026-10-23` (FMP Ultimate ~40% used, Polygon ~70%, the small keys 0–30%).

Doctrine that every bank follows (the 2026-09-02 warehouse rule applied to the paid providers):

* **Bank verbatim, on data.html.** Every response is stored as served (CSV or JSON, gzip) under `data/warm/<bank>/`,
  with sha256 / pulled_at / request facts in S3 metadata. The data is ours whatever happens to the subscription.
* **Record, never guess.** A plan refusal (401/402/403/426, or a provider error envelope) is written to the bank's
  catalog by code, the dataset is skipped for the day and re-probed tomorrow. Nothing is worked around.
* **Once a day, bounded, no self-chaining.** One Scheduler schedule per bank (cadence floor), one 780-second budget per
  invoke, cursors in state; the backfill is finished by `.github/workflows/fmp-backfill.yml` (daily, ≤ 320 min, ≤ 12
  lease-guarded re-kicks per engine, disables itself on `ALL_STEADY`).
* **Write-if-changed.** sha256 in metadata; an unchanged body is never re-put (no versions churn, LastModified stays a
  true change signal).
* **Measured coverage.** `justhodl-provider-coverage` folds the manifests into `data/provider-coverage.json` daily.

| Bank | Engine (schedule UTC) | Prefix | What it holds |
|---|---|---|---|
| FMP bulk + EOD | `justhodl-fmp-bank` (23:13) | `data/warm/fmp-bank/` | every `*-bulk` file FMP serves (profiles, ratings, DCF, scores, targets, consensus, TTM metrics/ratios, peers, ETF holders; statements + growth + key-metrics + ratios per fiscal year × annual/Q1–Q4 since 1985; earnings surprises per year) and `eod-bulk` per session since 2000 |
| FMP non-bulk | `justhodl-fmp-walker` (23:27) | `data/warm/fmp/` | ~180 datasets from `fmp-walker-spec.json` (v1.2.0: a daily-complete price lane refreshes the full EOD history of every chart name, US active stock/ETF, index, USD forex pair, top crypto and commodity every run, so price asks are answered at the last completed session; the long tail rotates): lists, calendars (earnings/dividends/splits/IPO/economic since 2000), SEC filing feeds, insider/13F/Senate/House/transcript/news latest feeds, per-symbol histories for the US universe (prices back to the 1960s, market cap, dividends, splits, earnings, estimates, targets, grades, ratings, as-reported statements, segmentation, executives, employees, float, ESG, insiders, 13F positions per quarter, transcripts per quarter), per-filer 13F (dates, performance, extracts), ETF holdings/weightings/dates, economics (23 indicator names, COT, treasury), sector/industry history, 1-minute bars per session for the chart universe |
| Polygon beyond the tape | `justhodl-polygon-bank` (22:41) | `data/warm/polygon-bank/` | market status/holidays, every ticker (active/inactive/OTC/crypto/fx/indices), exchanges/conditions/types, whole-market EOD snapshot + gainers/losers, indices snapshot, grouped crypto/fx since 2018, dividends/splits since 1990, short interest (entitled), ticker details/events/related, options contracts (incl. expired) for the chart universe, minute bars (730 days) for the chart universe, ETF Global flows/constituents/profiles for every US ETF |
| Small keyed providers | `justhodl-provider-bank` (21:53) | `data/warm/provider-bank/<provider>/` | Trading Economics (countries, indicators, markets ×5, calendar forward + since 2010, earnings, news, ratings + history, forecasts, comtrade, the full country × indicator history grid, markets history), CoinMarketCap (map, listings, categories, info, global metrics, fear & greed, exchanges, trending; plan-gated history probes), NewsAPI (sources, 12 countries' business headlines, 5 US categories, 20 macro queries), Nasdaq Data Link (entitled databases, every dataset of the priority databases), CryptoQuant (per-exchange / per-miner splits, hourly probes) |
| EIA full catalog | `justhodl-eia-bank` (21:31) | `data/warm/eia-bank/` | the whole EIA v2 route tree (weekly), every leaf's metadata, full history as 5,000-row offset pages per route × frequency, daily latest pages |
| Scoreboard | `justhodl-provider-coverage` (06:47) | `data/provider-coverage.json` | served / refused / untested / skipped per provider, GB banked, consumers |

Reading the banks from code: `aws/shared/fmp_bank.py` — `try_bank(path, qs)` answers a `/stable` path from the bulk
bank, then the walker bank (70+ paths); `statements()`, `snapshot()`, `eod()`, `walker(dataset, entity, period, day)`,
`walker_manifest()`. Bulk CSVs are whole-market files, so the reader decodes each one ONCE per container into an
indexed table (symbol → row spans; ~2.5 s for a 44 MB bulk, then ~0.1 ms per symbol) inside a memory budget of 20% of
the Lambda's memory (48–512 MB), remembers missing objects for 15 minutes, and reports `thrashing()` when a working set
does not fit. `fmp_book.try_harvest` calls it first, so every engine on `fmp_stable.fmp_get` / `equity_enrich.fmp`
reads the warehouse before the provider. Pages read the same objects through the Worker
(`https://justhodl.ai/data/warm/...`; gzip is transparent).

## The consumer lane — "pulled AND used" (2026-10-25)

Only 6 engines used the shared FMP client; 229 built their own `/stable` URLs. `aws/shared/fmp_bankfirst.py` is the
bank-first shim for them: `install()` (one guarded block at the top of `lambda_function.py`, inserted by
`scripts/fmp_bankfirst_apply.py`) wraps `urllib.request.urlopen` for `financialmodelingprep.com/stable/` URLs only.
**209 engines are bank-first** (batches 1–3; census in `docs/audit/2026-10-25/fmp-consumer-census.md`).

What the shim answers, and only that — the bank may never change the MEANING of a call:
* parameters it reproduces exactly: `symbol`, `limit`, `period`, `from`/`to` (applied to dated rows), plus the
  dataset's own key (`cik`, `name`, `exchange`, `year`+`quarter`). `page=`, `datatype=csv`, `structure=`, `symbols=`
  and anything unknown go live; statements need an explicit `limit` ≤ 12 annual / 16 quarterly.
* freshness: price / market-cap histories only when the banked newest `date` IS the last completed US session (never
  during the 13:30–21:30 UTC session for a current-dated ask — the partial bar is the provider's); fast families
  (estimates, grades, targets, insiders, Senate/House, news, earnings, dividends, splits, ETF holdings…) pulled today or
  yesterday (the walker stamps dates; its run is 23:27 UTC);
  slow walker families within 30 days; bulk-bank answers only while the bank manifest is ≤ 2 days old; quotes,
  searches, screeners, calendars, snapshots, treasury/market-risk never.
* cost: 25 consecutive misses on a dataset → that dataset is live for the rest of the 15-minute window; cache
  thrashing, a mean bank time above 0.3 s after 20 attempts, or 90 s of bank time → everything live (one log line
  `[fmp-bankfirst] bank stepped aside…`). Every engine logs `hits / stale / misses / unshaped / bypassed` through
  `print_stats()` or the counters in `fmp_bankfirst.STATS`; the response carries `X-JH-Source: fmp-bank`.
* never inside the banks or the provider probes (`NEVER_INSTALL_IN`: fmp-bank, fmp-walker, fmp-ratios,
  provider-catalog, provider-coverage, provider-bank, polygon-bank, eia-bank) — a bank fed its own output would freeze.

Not converted, and why (each is its own reviewed operation):
* byte-ledgered engines whose tests pin the source bytes or a complete-predecessor delta: `earnings-pead`,
  `microcap-float-squeeze`, `momentum-breakout`, `momentum-leaders`, `options-flow-scanner`, `telegram-bot`,
  `volatility-squeeze-hunter`, `squeeze-pretrigger`, `signal-logger` (ledgered patch = fixture + delta update in the
  same commit; the repo's append-below-the-predecessor convention means the install block goes at the END of those
  modules);
* `katlin` (candidate-managed: `$LATEST` deploys, the `live` alias is promoted by its own validated operation);
* `global-stress`, `velocity-acceleration` (pre-existing failing tests — deploy-blocked for any change until fixed);
* `fmp-fundamentals-agent` (requests, not urllib), `investor-agents` / `stock-ai-research` (urllib3) — a requests/urllib3
  adapter is the next shim if these matter; `brief-compiler` / `provider-catalog` only mention the host.

Deploy traps met on the way (all fixed in-repo): config `description` > 256 chars fails the whole preflight
(8 engines trimmed); a config `schedule` naming a classic rule that no longer exists fails the existing-schedule guard
(`mean-reversion`, `fundamentals-engine` → preserve-only Scheduler references; `cascade-validator` → the live rule name
and cron; `research-backtest` → cadence-only string, it has no live binding); byte-pinned fixtures are a lane of
their own.

Not pulled, on purpose (each bank's manifest carries the list): technical indicators and TTM statements (derived from
banked bars/quarters), `company-outlook` (a composite), custom DCF (parametric), `financial-reports-json/xlsx` (the SEC
XBRL lane, restored weekly 2026-10-23, holds the same filings), intraday quote snapshots (equal the EOD close once a
day), search endpoints, Polygon trades/quotes and Benzinga (not entitled), whole-market option chains (the options desks
pull their universes; a full chain is ~1.5 M contracts a day), websockets/flat files (Stocks Advanced only).

Open decisions (Khalid): Massive tier for trades/quotes and the Benzinga add-on; a Quiver key; the plan verdicts the
small providers record (each refusal on the coverage card is a plan line, not a bug); the ledgered patches for the
nine byte-pinned engines and the `katlin` live-alias promotion (above); the `estimate-revisions` calendar rewrite off
Benzinga.

Operations: birth ops 6640 (fmp-bank), 6646 (fmp-walker), 6647 (polygon-bank), 6648 (provider-bank), 6650 (eia-bank);
logs/state read-only op 6649. Every bank's `manifest.json` and `catalog.json` are public under its prefix.
