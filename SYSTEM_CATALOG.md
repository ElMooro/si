# JUSTHODL.AI — System Catalog

> **Authoritative system inventory.** Auto-generated from ops 1021 (2026-05-21). When Claude is asked to build a feature, **check this file first** — most features already exist.

## At a glance

- **333 Lambdas** across 21 families
- **235 EventBridge rules** active
- **24,604 S3 data keys** in `s3://justhodl-dashboard-live/data/` (incl. historical archives + snapshots)
- **200 HTML pages**, **180 linked** from main nav on index.html

## Hard constants

- AWS account: **857687956942**, region: **us-east-1**
- Lambda role: `arn:aws:iam::857687956942:role/lambda-execution-role`
- Scheduler role: `arn:aws:iam::857687956942:role/justhodl-scheduler-role`
- Runtime: **python3.12** for all Lambdas
- S3 bucket: **justhodl-dashboard-live**
- GitHub repo: **ElMooro/si** (main branch; auto-deploy via .github/workflows)
- Site: **justhodl.ai** (GitHub Pages from root *.html)
- API: **api.justhodl.ai** (Cloudflare Worker route)

## CRITICAL build doctrine

Before building ANY feature, Lambda, or page:

1. **Search this file** for the name or feature space
2. **Audit deployed state**: does Lambda exist? S3 output present? EventBridge scheduled?
3. **If exists** → patch what's broken; do not rebuild
4. **Document audit findings** before writing new code
5. **Memory pattern**: many features Claude assumed to be new (squeeze stack, 13F stack, PEAD stack) were already built. Only ~1 in 4 proposed Phase D candidates was a true net-new gap.

## Lambda inventory by family

### 13F / Activist / SEC (5)

- `justhodl-13f-positions`
- `justhodl-13f-price-divergence`
- `justhodl-activist-13d`
- `justhodl-activist-filings-scanner`
- `justhodl-sec-13f`

### AI / Agent (5)

- `justhodl-ai-chat`
- `justhodl-charts-agent`
- `justhodl-investor-agents`
- `justhodl-ultimate-orchestrator`
- `justhodl-valuations-agent`

### Bagger pack (4)

- `justhodl-bagger-engine`
- `justhodl-coffee-can`
- `justhodl-hiring-velocity`
- `justhodl-insider-aggregate`

### Bonds / Credit / Rates (7)

- `justhodl-bond-regime-detector`
- `justhodl-bond-trace`
- `justhodl-buyback-yield-ranking`
- `justhodl-commodity-curves`
- `justhodl-credit-equity-divergence`
- `justhodl-credit-stress`
- `justhodl-yield-curve`

### CFTC / COT (2)

- `justhodl-cot-extremes-scanner`
- `justhodl-cot-tracker`

### Catalyst / Calendar (3)

- `justhodl-catalyst-calendar`
- `justhodl-econ-calendar`
- `justhodl-spinoff-desk`

### Crisis / Stress (10)

- `justhodl-bank-stress`
- `justhodl-cds-monitor`
- `justhodl-crisis-knowledge-base`
- `justhodl-crisis-plumbing`
- `justhodl-firm-stress`
- `justhodl-global-stress`
- `justhodl-stress-loadings`
- `justhodl-stress-scenarios`
- `justhodl-stress-simulator`
- `justhodl-systemic-stress`

### Crypto (8)

- `justhodl-crypto-enricher`
- `justhodl-crypto-etf-arb`
- `justhodl-crypto-funding`
- `justhodl-crypto-intel`
- `justhodl-crypto-narratives`
- `justhodl-crypto-opportunities`
- `justhodl-dex-scanner`
- `justhodl-index-recon`

### Data infra (7)

- `justhodl-analyst-consensus`
- `justhodl-calibration-snapshotter`
- `justhodl-financial-secretary`
- `justhodl-fred-proxy`
- `justhodl-health-monitor`
- `justhodl-history-api`
- `justhodl-history-snapshotter`

### Insider / Smart Money (9)

- `justhodl-insider-buyback-confluence`
- `justhodl-insider-buys-enriched`
- `justhodl-insider-cluster-scanner`
- `justhodl-insider-sell-cluster`
- `justhodl-insider-trades`
- `justhodl-rating-change-cluster`
- `justhodl-smart-money-cluster`
- `justhodl-smart-money-holdings`
- `justhodl-smart-money-tracker`

### Macro / Fed / Liquidity (18)

- `justhodl-auction-crisis-detector`
- `justhodl-auction-grader`
- `justhodl-boj-detail`
- `justhodl-cb-injection`
- `justhodl-china-liquidity`
- `justhodl-ecb-detail`
- `justhodl-ecb-proxy`
- `justhodl-eurodollar-stress`
- `justhodl-fed-speak`
- `justhodl-global-liquidity`
- `justhodl-liquidity-agent`
- `justhodl-liquidity-capacity`
- `justhodl-liquidity-credit-engine`
- `justhodl-liquidity-flow`
- `justhodl-liquidity-profile`
- `justhodl-liquidity-pulse`
- `justhodl-nyfed-dealer-survey`
- `justhodl-snb-detail`

### Other / Misc (150)

- `justhodl-52wk-quality-breakout`
- `justhodl-ab-test`
- `justhodl-activity-nowcast`
- `justhodl-alert-router`
- `justhodl-alpha-alerts`
- `justhodl-alpha-calibrator`
- `justhodl-alpha-confluence`
- `justhodl-alpha-score`
- `justhodl-anomaly-detector`
- `justhodl-api-keys-admin`
- `justhodl-backtest-engine`
- `justhodl-backtest-harness`
- `justhodl-beta-laggard`
- `justhodl-bloomberg-v8`
- `justhodl-breadth-divergence`
- `justhodl-breadth-thrust`
- `justhodl-buyback-scanner`
- `justhodl-calibration-fleet`
- `justhodl-calibration-snapshot`
- `justhodl-calibrator`
- `justhodl-calls-backtest`
- `justhodl-canary-grid`
- `justhodl-capital-return`
- `justhodl-cb-stance`
- `justhodl-cdn-diag-temp`
- `justhodl-cds-proxy`
- `justhodl-cef-discount`
- `justhodl-chart-data`
- `justhodl-chart-patterns`
- `justhodl-construction-housing`
- `justhodl-consumer-pulse`
- `justhodl-conviction-engine`
- `justhodl-correlation-breaks`
- `justhodl-correlation-surface`
- `justhodl-cross-asset-regime`
- `justhodl-cross-asset-rv`
- `justhodl-cta-trend-exhaust`
- `justhodl-data-collector`
- `justhodl-debate-engine`
- `justhodl-desk-returns`
- `justhodl-divcut-warning`
- `justhodl-divergence-engine-v2`
- `justhodl-divergence-interpreter`
- `justhodl-divergence-scanner`
- `justhodl-dividend-growth`
- `justhodl-dollar-radar`
- `justhodl-dxy-equity-divergence`
- `justhodl-earnings-iv-crush`
- `justhodl-earnings-nlp`
- `justhodl-earnings-tracker`
- `justhodl-edge-engine`
- `justhodl-esi`
- `justhodl-euro-fragmentation`
- `justhodl-event-study`
- `justhodl-failed-pattern-reversal`
- `justhodl-feedback`
- `justhodl-firm-book`
- `justhodl-fleet-monitor`
- `justhodl-forensic-screen`
- `justhodl-fundamentals-engine`
- `justhodl-fx-intelligence`
- `justhodl-gap-fill-confirm`
- `justhodl-global-business-cycle`
- `justhodl-global-macro`
- `justhodl-global-markets`
- `justhodl-gsi-calibrator`
- `justhodl-gsi-horizons`
- `justhodl-historical-analogs`
- `justhodl-implied-prob`
- `justhodl-intelligence`
- `justhodl-ka-metrics`
- `justhodl-khalid-adaptive`
- `justhodl-khalid-metrics`
- `justhodl-labor-leading`
- `justhodl-leading-markets`
- `justhodl-live-pulse`
- `justhodl-lockup-expiration`
- `justhodl-ma-tracker`
- `justhodl-margin-lending`
- `justhodl-market-extremes`
- `justhodl-market-internals`
- `justhodl-mean-reversion`
- `justhodl-merger-arb`
- `justhodl-metals-miners`
- `justhodl-ml-predictions`
- `justhodl-momentum-breakout`
- `justhodl-momentum-scanner`
- `justhodl-multi-tf-convergence`
- `justhodl-narrative-density-tracker`
- `justhodl-ndx-spx-spread`
- `justhodl-news-velocity`
- `justhodl-nobrainer-rationale`
- `justhodl-nobrainer-tracker`
- `justhodl-oecd-cli`
- `justhodl-onchain-ratios`
- `justhodl-opportunity-calibrator`
- `justhodl-opportunity-engine`
- `justhodl-outcome-checker`
- `justhodl-pairs-arb`
- `justhodl-pairs-scanner`
- `justhodl-plumbing-aggregator`
- `justhodl-pnl-attribution`
- `justhodl-pnl-tracker`
- `justhodl-political-trades`
- `justhodl-position-monitor`
- `justhodl-position-sizer-v2`
- `justhodl-pre-pump-detector`
- `justhodl-price-redundancy`
- `justhodl-prompt-iterator`
- `justhodl-public-api-demo`
- `justhodl-push-api`
- `justhodl-put-call-extreme`
- `justhodl-redflag-alerter`
- `justhodl-regime-anomaly`
- `justhodl-reit-nav-discount`
- `justhodl-repo-lending`
- `justhodl-repo-monitor`
- `justhodl-reports-builder`
- `justhodl-revenue-acceleration`
- `justhodl-reversal-radar`
- `justhodl-russell-recon-frontrun`
- `justhodl-rv-iv-scanner`
- `justhodl-seasonality`
- `justhodl-sec-10kq`
- `justhodl-sec-8k`
- `justhodl-sellside-views`
- `justhodl-signal-logger`
- `justhodl-signal-orthogonality`
- `justhodl-signal-scorecard`
- `justhodl-spac-floor-warrant`
- `justhodl-stealth-accumulation`
- `justhodl-streaming-fanout`
- `justhodl-supply-inflection-scanner`
- `justhodl-system-signal-logger`
- `justhodl-tenor-signal-interpreter`
- `justhodl-tmp-433`
- `justhodl-tmp-454`
- `justhodl-tmp-458`
- `justhodl-tmp-force-refresh`
- `justhodl-track-record`
- `justhodl-trade-evaluator`
- `justhodl-trade-journal`
- `justhodl-trade-logger`
- `justhodl-treasury-proxy`
- `justhodl-trend-engine`
- `justhodl-universe-builder`
- `justhodl-vrp`
- `justhodl-wave-signal-logger`
- `justhodl-whats-changed`
- `justhodl-yen-carry`

### PEAD / Earnings (6)

- `justhodl-earnings-pead`
- `justhodl-earnings-quality`
- `justhodl-earnings-whisper`
- `justhodl-eps-revision-velocity`
- `justhodl-pead-detector`
- `justhodl-post-earnings-mean-rev`

### Pro Pack v3 (9)

- `justhodl-beneish`
- `justhodl-bond-vol`
- `justhodl-eva-spread`
- `justhodl-gf-value`
- `justhodl-ipo-pipeline`
- `justhodl-magic-formula`
- `justhodl-predictability`
- `justhodl-smart-beta`
- `justhodl-starmine`

### Reports / Brief / Email (8)

- `justhodl-ai-brief`
- `justhodl-alpha-daily-brief`
- `justhodl-daily-report-v3`
- `justhodl-email-reports`
- `justhodl-email-reports-v2`
- `justhodl-morning-brief-tg`
- `justhodl-morning-intelligence`
- `justhodl-telegram-bot`

### Risk / Hedge / Portfolio (26)

- `justhodl-allocator`
- `justhodl-cro-digest`
- `justhodl-cro-escalation`
- `justhodl-daily-macro-report`
- `justhodl-desk-allocator`
- `justhodl-factor-risk`
- `justhodl-firm-risk-board`
- `justhodl-hedge-planner`
- `justhodl-hedge-pnl`
- `justhodl-macro-nowcast`
- `justhodl-macro-surprise`
- `justhodl-master-allocator`
- `justhodl-merger-arb-risk`
- `justhodl-pm-decision`
- `justhodl-portfolio-admin`
- `justhodl-portfolio-catalysts`
- `justhodl-portfolio-risk`
- `justhodl-portfolio-sizer`
- `justhodl-portfolio-snapshot`
- `justhodl-retail-sentiment`
- `justhodl-risk-monitor`
- `justhodl-risk-radar`
- `justhodl-risk-sizer`
- `justhodl-signal-portfolio`
- `justhodl-skew-tail-hedging`
- `justhodl-tail-hedge`

### Screener / Watchlist (8)

- `justhodl-deep-value-screener`
- `justhodl-opportunity-screener`
- `justhodl-screener-alerts`
- `justhodl-stock-ai-research`
- `justhodl-stock-analyzer`
- `justhodl-stock-screener`
- `justhodl-watchlist`
- `justhodl-watchlist-debate`

### Sector / Theme (9)

- `justhodl-gold-equity-rotation`
- `justhodl-sector-earnings-diffusion`
- `justhodl-sector-heatmap`
- `justhodl-sector-rotation`
- `justhodl-sector-tilt`
- `justhodl-sympathetic-momentum`
- `justhodl-theme-detector`
- `justhodl-theme-rotation-engine`
- `justhodl-theme-tier-classifier`

### Sentiment / Composite (12)

- `justhodl-aaii-sentiment`
- `justhodl-asymmetric-hunter`
- `justhodl-asymmetric-scorer`
- `justhodl-best-ideas`
- `justhodl-compound-aggregator`
- `justhodl-crisis-composite`
- `justhodl-earnings-sentiment`
- `justhodl-gdelt-sentiment`
- `justhodl-master-ranker`
- `justhodl-news-sentiment`
- `justhodl-regime-composite`
- `justhodl-signal-board`

### Squeeze stack (6)

- `justhodl-finra-short`
- `justhodl-microcap-float-squeeze`
- `justhodl-short-interest`
- `justhodl-short-pressure`
- `justhodl-squeeze-pretrigger`
- `justhodl-volatility-squeeze-hunter`

### Tape / Flow / Vol (21)

- `justhodl-catalyst-skew-premove`
- `justhodl-dealer-gex`
- `justhodl-dix`
- `justhodl-etf-flows`
- `justhodl-exchange-flows`
- `justhodl-opex-calendar`
- `justhodl-options-flow`
- `justhodl-options-flow-scanner`
- `justhodl-options-gamma`
- `justhodl-precatalyst-vol-expansion`
- `justhodl-stablecoin-flow`
- `justhodl-tape-reader`
- `justhodl-tic-flows`
- `justhodl-vix-backwardation-trigger`
- `justhodl-vix-curve`
- `justhodl-vix9d-vix-inversion`
- `justhodl-vol-radar`
- `justhodl-vol-regime`
- `justhodl-vol-surface`
- `justhodl-vol-target-unwind`
- `justhodl-vvix-vov-regime`

## Page inventory (200 HTML pages)

### Pro Pack v3 cockpits (institutional layer)

- `eva.html` — Stern Stewart EVA Spread (#10)
- `predictability.html` — GuruFocus Predictability (#7)
- `smart-beta.html` — MSCI Smart Beta 4-factor (#8)
- `gf-value.html` — GF Value / Damodaran (#1)
- `magic-formula.html` — Greenblatt Magic Formula (#3)
- `starmine.html` — Refinitiv StarMine (#4)
- `beneish.html` — Beneish M-Score forensic (#6)
- `bond-vol.html` — Synthetic MOVE / bond vol (#5)
- `ipo-pipeline.html` — IPO Pipeline (#2)
- `squeeze.html` — 5-engine squeeze cockpit
- `retail-edges.html` — 33-engine Tier 1-5 retail edges
- `best-ideas.html` — 20-engine confluence capstone

### Other major institutional pages

- `signal-board.html` — Unified cross-asset signal store
- `13f.html` — 13F holdings tracker
- `activist-13d.html` — 17-activist 13D scanner (EDGE #9)
- `smart-money.html` — Smart-money clusters
- `pead-signals.html` — Post-Earnings Announcement Drift
- `baggers.html` — 100x bagger DNA
- `risk-desk.html` — 14-engine CRO cockpit
- `portfolio-manager.html` — Position book + manager actions
- `master-ranker.html` — Cross-engine name rank
- `catalyst-calendar.html` — Earnings/FDA/index changes
- `chart-pro.html` — Bloomberg-style chart
- `screener/` — PROTECTED S&P 500 screener (never delete)
- `why.html` — Cross-signal anomaly tracer

## Data sources & known status

### Working
- **FRED** (api.stlouisfed.org): 252+ series via `data/fred-cache.json` (smart TTL, 88% hit rate)
- **FMP `/stable/` endpoints**: fully unlocked (segmentation, DCF, scores, insider, senate, calendars, ratios, key-metrics-ttm)
- **AlphaVantage**: free tier (rate-limited)
- **Polygon stocks tier**: OHLCV + grouped daily + reference + short-volume aggregates
- **CMC** (CoinMarketCap): categories + fear-greed
- **CFTC**: COT (TFF + Disagg + Legacy)
- **NY Fed**: dealer survey + auction data
- **CoinAPI / CoinGecko**: crypto OHLCV
- **EDGAR**: SEC filings + 13F (via FMP /stable/)
- **FINRA Reg SHO daily file**: short-volume CDN (working, `justhodl-finra-short`)

### Dead / broken upstreams (DO NOT USE)
- **FMP `/api/v3/` and `/api/v4/`**: Legacy 403 since 2025-08-31; MUST use `/stable/`
- **Polygon `/stocks/v1/short-interest`**: largely dead post-2018 (130 of 157 tickers stuck at 2018-05-15 even with order=desc). REPLACEMENT IN-FLIGHT: `aws/shared/finra_si.py` scaffold awaits Khalid's FINRA Gateway registration.
- **CBOE Put/Call ratio feed**: dead 2026-05; replaced by Sentiment Extreme Composite v2.0 (Baker-Wurgler 5-source FRED z-score)

### Critical FMP /stable/ field names (per ops 1006 + 1012 probes)

Endpoint → field gotchas. Many `/stable/` endpoints do **not** carry the field name you expect from training data.

- `/stable/quote` has **no `pe`** field — get PE from `/stable/ratios-ttm`
- `/stable/ratios-ttm`: PE = **`priceToEarningsRatioTTM`** (NOT `peRatioTTM`)
- `/stable/ratios-ttm`: PB = **`priceToBookRatioTTM`**
- `/stable/key-metrics-ttm`: ROIC = **`returnOnInvestedCapitalTTM`** (NOT `roicTTM`)
- `/stable/ratios-ttm`: Gross margin = `grossProfitMarginTTM`

## API keys (in env vars; do not log)

Stored as Lambda env vars. Donor pattern uses `inherit_env` list of `{from_function, keys}` entries. Common donors:
- `justhodl-starmine` donates `FMP_KEY`
- `justhodl-cross-asset-rv` donates `FRED_KEY`
- `justhodl-vol-surface` donates `TELEGRAM_*`
- `justhodl-finra-short` donates `POLY_KEY`

Keys (also in memory edit #5, kept here for self-containment):
- FMP: `wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb` (**`/stable/` only**)
- FRED: `2f057499936072679d8843d7fce99989`
- Polygon: `zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d`
- AlphaVantage: `EOLGKSGAYZUXKPUL`
- CMC: `17ba8e87-53f0-46f4-abe5-014d9cd99597`
- BLS: `a759447531f04f1f861f29a381aab863`
- BEA: `997E5691-4F0E-4774-8B4E-CAE836D4AC47`
- Census: `8423ffa543d0e95cdba580f2e381649b6772f515`
- Telegram bot: `8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs`, chat_id `8678089260` at `/justhodl/telegram/chat_id`

## PROTECTED / DO NOT TOUCH WITHOUT EXPLICIT APPROVAL

- **`/screener/` page + `justhodl-stock-screener` Lambda** — Khalid loves this; exclude from all mass migrations.
- **`justhodl-best-ideas` SPECS list** — 20-engine confluence; extend, don't rewrite.
- **`justhodl-signal-board`** — synthesis layer; aggregates 7+ engines; do not modify scoring weights without testing.
- **Pro Pack v3 #1-10 Lambdas + their cockpit HTML pages** — all verified ALL_PASS via ops 998-1009.

## Ship loop reminder

1. `cd ~/work/si; edit; git add X; git -c user.email=raafouis@gmail.com -c user.name=Khalid commit -m msg; git push`
2. 4 deploy paths:
    - root `*.html` → GitHub Pages → ~30s
    - `aws/lambdas/*/source/**` → `deploy-lambdas.yml`
    - `cloudflare/workers/*/**` → `deploy-workers.yml`
    - `aws/ops/pending/**` → `run-ops.yml`
3. Sandbox blocks `*.amazonaws.com` — NEVER run aws CLI/boto3 locally; use ops-pending scripts that run in CI
4. GH Actions diff = HEAD^ HEAD — split unrelated changes across commits
5. Always `\n` line endings; CRLF breaks Lambda
6. Lambda Description ≤250 chars

## Audit doctrine (per memory entry #29)

After meaningful push, write `ops/pending/NNN_X_verify.py` = create temp Lambda that fetches live URL → scans markers/regex/counts → returns JSON → write `ops/reports/NNN.json`; delete temp Lambda; commit/push, sleep 100-300s, git pull, parse report. Claude proves work end-to-end without Khalid running anything.

## Anthropic model

Use `claude-haiku-4-5-20251001` for all Lambdas calling Anthropic API. `claude-3-haiku-20240307` was retired (400 errors).

---

## 2026-05-31 — Major Forward-Intel Expansion (this session)

Two new Lambdas + three v3 upgrades + five dedicated dashboards landed in a single
multi-pass session. Total: 6 Lambdas touched, 5 HTML pages added, 5 new ops scripts
(1046–1055).

### New Lambdas

**`justhodl-sec-filings-intel`** — `cron(0 9,15,21 * * ? *)` · 256MB / 600s
Comprehensive SEC EDGAR full-text scanner across 14 institutional alpha signals.
First run: 364 events scanned, 276 tickers with signals, 20 critical-severity.
BEARISH weights: bankruptcy (-50), going concern (-40), restatement (-30),
material weakness (-25), investigation (-22), auditor change (-20), CFO departure
(-15), ATM offering (-10), bought deal (-5). BULLISH: M&A definitive (+30), FDA
approval (+25), going-private (+25), buyback (+12), strategic partnership (+8).
Emits `sec_filings.material_event` for critical+high severity only (formatter
filters lower).

**`justhodl-political-stocks`** v1.3 — `cron(0 14 * * ? *)` · 512MB / 300s
S3-cache-first architecture. Three resilience layers:
- Party map: `data/congress-party-map.json` (S3 → live → 39-entry hardcoded)
- Trades: live Quiver → `data/quiver-congress-cache.json` (S3 fallback)
- Output: `data/political-stocks.json` schema 1.3 with `quiver_source` provenance
Live state: 1000 trades, 283 tickers, 25 clusters, 14 bipartisan buys,
536 party mappings (D=260 / R=273 / I=3). Top bipartisan: MSFT (R×5 D×6, +230),
HD (R×6 D×3, +180), PH, UNH, GE. Trump holdings: 4 positions from 2025-03-19 OGE
278e (DJT controlling, T-Bills, $TRUMP coin, Trump Org).

### v3 Upgrades to existing engines

**`justhodl-forward-orders`** — schema 3.0 — added 2 subscores:
- `rpo_acceleration` 15% — multi-quarter QoQ trend (acceleration_pp =
  qoq_recent − qoq_prior). Positive = backlog growing FASTER than last quarter.
- `peer_percentile` 5% — sector-relative rank via two-pass scoring
WEIGHTS rebalanced: yield 30 / growth 25 / accel 15 / contracts 15 / B2B 10 / peer 5

**`justhodl-rotation-chain`** v2 — added per-tier:
- `tier_breadth_30d` (% of tier members with positive returns)
- `volume_confirmation_20d` (recent 20d avg vol / prior 20d)
- `rotation_confidence` 0-100: +25 if leader breadth ≥75%, +15 if volume ≥1.3x
Distinguishes real rotation from HFT noise.

**`justhodl-buzz-velocity`** v3 — added:
- `lightweight_sentiment()` rule-based on ~25 bull/25 bear keywords on Reddit+News
  title sample. Returns score in [-1,+1].
- `divergence` detection: negative_divergence = velocity ≥1.8x + 7d price ≤-8%
  (attention up, price tanking = warning). positive_divergence = price ≥+12% but
  sentiment <-0.2.
- Score adj: +8 sentiment ≥0.4, -8 sentiment ≤-0.4.

### Event coordinator — 2 new routes

- `sec_filings.material_event` — suppresses non-critical/non-high in formatter
- `political.cluster_buy` — suppresses single-party clusters under 4 politicians;
  bipartisan flag always triggers regardless of cluster size

### Five new dashboard tabs

All linked from `index.html` between Future Intel and Opportunities:
- `/forward-orders.html` — 5-stat hero · top-30 table · 6 sub-bars · contract callouts
- `/rotation-chains.html` — 11 chain cards by state (ROTATING/SYNC/DIV) · per-tier
  perf+breadth+vol grid · next-up ticker rows with lag · confidence score
- `/buzz-velocity.html` — STEALTH/DIVERGENCE/EXTREME panels · top-30 with sentiment
  badges + divergence tags
- `/sec-filings.html` — 14-signal legend · CRITICAL/RISKS/OPPORTUNITIES panels ·
  full ticker table with event pills per row
- `/political.html` — Trump holdings card · 6-stat hero · TOP BUYS/CLUSTERS/
  BIPARTISAN/TOP SELLS panels · party tags (D×N R×N format)

### Notable debugging arc (worth knowing for next time)

1. House/Senate Stock Watcher S3 buckets at house-stock-watcher-data.s3-us-west-2
   went HTTP 403 — community project shut down public data (ops/1047).
2. Capitol Trades BFF API: 503 from AWS us-east-1 (Cloudflare blocks DC IPs) (1048).
3. Quiver Quant probe: `/beta/live/congresstrading` works no-auth, 1000 recent
   trades, 430KB (1049). Bonus: `/beta/live/lobbying` works (20K records — future
   signal source).
4. `theunitedstates.io/congress-legislators/legislators-current.json` blocks AWS
   us-east-1 IPs (Errno 110 timeout) — used since 2026 (1052-1053).
5. GitHub `main` branch returns 404 for `.json` files — that repo maintains YAML
   on main; JSON files are auto-built and live on `gh-pages` (1054-1055).
6. ⚠ Quiver rate-limits repeat calls within seconds from same VPC IP. Mitigation:
   S3-cache-first pattern with live fallback in the Lambda.

### Future signal source identified but not yet wired

`api.quiverquant.com/beta/live/lobbying` — 20K records, no auth. Format:
{Date, Amount, Client, Issue, Specific_Issue, Registrant, Ticker}. Strong
forward-intelligence data (companies don't lobby hard on issues that don't
matter to them; lobbying expenditure often precedes policy/regulatory change
that moves prices). Candidate engine: `justhodl-lobbying-intel` — flag companies
with rising lobbying spend by issue category.

_Generated by ops 1021 on 2026-05-21. Refresh by re-running `aws/ops/pending/1021_system_full_audit.py` and re-running this generator._
