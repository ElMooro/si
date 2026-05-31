# Dead Lambda Classifier v2 — strict regex + multi-signal

Generated: 2026-05-31T13:47:35.999840+00:00

Total classified: **367**

## v1 → v2 comparison

- v1 flagged 25 as 'CANDIDATE_DELETE_DEPRECATED' (mostly false positives)
- v2 with strict matching:
  - SAFE_TO_DELETE: **0** (high confidence)
  - DELETE_CANDIDATE: **0** (review then delete)
  - INVESTIGATE: **0** (mixed signals)
  - KEEP: **361** (clear keep signals)
  - PROTECTED: **6** (operator-declared do-not-touch)

## Classification Methodology

Each Lambda scored on opposing signals:
- **+2 DELETE**: name has \btest\b / \btmp\b / \bscratch\b (word-boundary regex)
- **+2 DELETE**: newer versioned sibling exists (engine-v1 when -v2 present)
- **+1 DELETE**: description says 'deprecated' / 'obsolete' / 'do not use'
- **+1 DELETE**: code size < 500 bytes (likely stub)
- **+3 KEEP**: has function URL (HTTP callable from outside)
- **+3 KEEP**: has event source mapping (DynamoDB stream etc.)
- **+3 KEEP**: >50 invocations in last 30 days
- **+2 KEEP**: EventBridge rule targets it
- **+1 KEEP**: modified in last 14 days (active development)
- **PROTECTED**: name in operator's do-not-touch list (~30 Lambdas)

Net score >= +3 with no keep signals → SAFE_TO_DELETE
Net score >= +1 → DELETE_CANDIDATE
Net score == 0 → INVESTIGATE


## KEEP (361)

| Name | Net | Delete signals | Keep signals | Description |
|------|----:|----------------|--------------|-------------|
| `justhodl-ab-test` | -1 | NAME_MARKER:\btest\b | ACTIVE:29_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | A/B test of competing prompt strategies |
| `justhodl-email-reports` | -1 |  | RECENTLY_MODIFIED:0.8d_ago |  |
| `justhodl-transcript-query` | -1 |  | RECENTLY_MODIFIED:0.8d_ago | Transcript Query Handler (R2b). On-demand TF-IDF ranked search over data/transcr |
| `justhodl-behavior-mirror` | -2 |  | SOME_ACTIVITY:4_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Exponential Idea #4 — Behavior Mirror. Compares system signals to actual portfol |
| `justhodl-beneish` | -2 |  | SOME_ACTIVITY:3_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Beneish M-Score (GuruFocus fraud-detection gap-closer): 8-variable composite ide |
| `justhodl-breadth-thrust` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Zweig Breadth Thrust + Whaley January Barometer + Coppock Curve. 11-of-11 histor |
| `justhodl-buyback-scanner` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Buyback authorization + drift scanner (Edge #6). Pulls SEC 8-K repurchase filing |
| `justhodl-buyback-yield-ranking` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | TTM buyback yield ranking. 5-factor quality: gross+net yield, FCF coverage>=1x,  |
| `justhodl-causality-scanner` | -2 |  | SOME_ACTIVITY:5_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Exponential Idea #3 — Auto-Causality Discovery. Granger causality across all pla |
| `justhodl-consensus-bottom` | -2 |  | SOME_ACTIVITY:5_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Quality-Filtered Consensus Bottom. AND-gate of bullish 13F divergence + Predicta |
| `justhodl-cta-trend-exhaust` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | CTA / Lev Fund trend exhaustion in equity index futures (ES+NQ+RTY+YM). CFTC TFF |
| `justhodl-dep-graph` | -2 |  | SOME_ACTIVITY:4_invokes_30d, RECENTLY_MODIFIED:2.7d_ago | Arch #8 — Platform Dependency Graph. Daily-eve: scans 374 Lambdas + 200+ pages,  |
| `justhodl-divcut-warning` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | tier2-retail-edges/divcut-warning |
| `justhodl-earnings-quality` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Earnings quality scanner via Sloan accruals + Beneish proxies. 4-factor: accrual |
| `justhodl-engine-contribution` | -2 |  | SOME_ACTIVITY:4_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | COUNTERFACTUAL ENGINE CONTRIBUTION — measures marginal portfolio PnL per engine  |
| `justhodl-factor-decomposition` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Factor Decomposition (R10). FF5+MOM 6-factor OLS regression per ticker over 60mo |
| `justhodl-feed-catalog` | -2 |  | SOME_ACTIVITY:4_invokes_30d, RECENTLY_MODIFIED:2.7d_ago | Arch #5 — Feed Catalog + JSON Schemas. Generates data/feed-catalog.json daily: e |
| `justhodl-gsi-horizons` | -2 |  | SOME_ACTIVITY:4_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Multi-Horizon GSI calibrator -- fits IC + weights at 5/21/63/252-day forward hor |
| `justhodl-insider-buys-enriched` | -2 |  | SOME_ACTIVITY:3_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Institutional enrichment of insider-cluster-scanner output. Per-cluster expected |
| `justhodl-opex-calendar` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | OPEX/0DTE gamma pinning calendar (Edge #8). Classifies trading day into OPEX reg |
| `justhodl-reit-nav-discount` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | tier2-retail-edges/reit-nav-discount |
| `justhodl-russell-recon-frontrun` | -2 |  | SOME_ACTIVITY:4_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Russell index reconstitution front-runner (Edge #5). Predicts FTSE Russell R1000 |
| `justhodl-rv-iv-scanner` | -2 |  | SOME_ACTIVITY:1_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | RV-IV Variance Risk Premium + Implied Dispersion scanner (Edge #10). FRED single |
| `justhodl-signal-orthogonality` | -2 |  | SOME_ACTIVITY:4_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Signal Orthogonality -- pairwise correlation + cluster audit across the fleet hi |
| `justhodl-spac-floor-warrant` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | tier2-retail-edges/spac-floor-warrant |
| `justhodl-stablecoin-flow` | -2 |  | SOME_ACTIVITY:1_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Stablecoin mint/flow tracker (Edge #7). Aggregates 15+ USD stables via DefiLlama |
| `justhodl-vix-backwardation-trigger` | -2 |  | SOME_ACTIVITY:2_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Institutional once-per-cycle capitulation buy signal. State machine NULL->WARM-> |
| `justhodl-vol-surface` | -2 |  | SOME_ACTIVITY:4_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Vol Surface — Yahoo Finance IV chains + BS delta. Term + RR25 + BF25 + risk-neut |
| `openbb-system2-api` | -2 |  | SOME_ACTIVITY:4_invokes_30d, RECENTLY_MODIFIED:9.1d_ago |  |
| `bls-employment-api-v2` | -3 |  | ACTIVE:12_invokes_30d, RECENTLY_MODIFIED:9.1d_ago |  |
| `daily-liquidity-report` | -3 |  | ACTIVE:31_invokes_30d, RECENTLY_MODIFIED:9.1d_ago |  |
| `ecb-data-daily-updater` | -3 |  | ACTIVE:31_invokes_30d, RECENTLY_MODIFIED:9.1d_ago |  |
| `justhodl-13f-price-divergence` | -3 |  | ACTIVE:8_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | retail-edges-tier3/13f-price-divergence |
| `justhodl-52wk-quality-breakout` | -3 |  | ACTIVE:12_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | tier2-retail-edges/52wk-quality-breakout |
| `justhodl-activist-13d` | -3 |  | ACTIVE:12_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Activist 13D scanner (Edge #9). Crawls SEC EDGAR per-filer JSON for 17 curated a |
| `justhodl-activity-nowcast` | -3 |  | ACTIVE:20_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Real-Time Activity Nowcast — high-frequency FRED basket into a 0-100 activity in |
| `justhodl-alpha-daily-brief` | -3 |  | ACTIVE:22_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Claude-synthesized morning brief (#5). Reads alpha-score+confluence+regime+senti |
| `justhodl-asymmetric-hunter` | -3 |  | ACTIVE:31_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Layer 4 of nobrainer hunter — fuses Layers 1+2+3 into 5-factor asymmetric score |
| `justhodl-asymmetric-scorer` | -3 |  | ACTIVE:25_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | Phase 2B — Asymmetric reward/risk equity scorer |
| `justhodl-auction-interpreter` | -3 |  | ACTIVE:8_invokes_30d, RECENTLY_MODIFIED:0.8d_ago | AI institutional auction brief — reads auction-crisis tape + cross-context regim |

*… and 321 more (see JSON)*

## PROTECTED (6)

| Name | Net | Delete signals | Keep signals | Description |
|------|----:|----------------|--------------|-------------|
| `justhodl-cross-asset-regime` | -10 |  | NAME_IN_PROTECTED_LIST |  |
| `justhodl-cross-asset-rv` | -10 |  | NAME_IN_PROTECTED_LIST | Cross-Asset Relative Value engine — OLS residual-z dislocation detector across 6 |
| `justhodl-crypto-intel` | -10 |  | NAME_IN_PROTECTED_LIST |  |
| `justhodl-portfolio-admin` | -10 |  | NAME_IN_PROTECTED_LIST | Portfolio CRUD (#9). Invoked manually with action+payload to manage POSITION/WAT |
| `justhodl-stock-analyzer` | -10 |  | NAME_IN_PROTECTED_LIST |  |
| `justhodl-stock-screener` | -10 |  | NAME_IN_PROTECTED_LIST |  |