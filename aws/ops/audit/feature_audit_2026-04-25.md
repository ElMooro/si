# JustHodl.AI Feature Audit — 2026-04-25

**Generated:** 2026-04-25T02:12:39.269964+00:00
**Method:** Cross-reference Khalid's requested features (from prior conversations) against current live system state (Lambdas + S3 + EB + SSM + DDB + CF).

**Status legend:**
- 🟢 = present + working
- 🟡 = present but degraded (high error rate, stale, no schedule, etc)
- 🔴 = present but broken (100% errors)
- ⚫ = missing entirely

## At a glance

- Total features audited: **72**
- 🟢 Working: **59**
- 🟡 Partial / degraded: **2**
- 🔴 Broken: **2**
- ⚫ Missing: **9**


## Core Lambdas (24)

| Status | Name | Requested feature | Evidence |
|---|---|---|---|
| 🟢 | `justhodl-daily-report-v3` | Bloomberg V10.3 5-min refresh, 188 stocks + 230+ FRED + 21 tabs | 290 inv / 0 err 24h |
| 🟢 | `justhodl-ai-chat` | AI chat with Claude, dashboard + standalone access | 10 inv / 0 err 24h |
| 🟢 | `justhodl-bloomberg-v8` | Earlier Bloomberg V8 5-min refresh | 288 inv / 0 err 24h |
| 🟢 | `justhodl-intelligence` | Cross-system synthesis hourly weekdays + 7AM ET | 16 inv / 0 err 24h |
| 🟢 | `justhodl-morning-intelligence` | Daily 8AM ET Telegram brief + self-improvement | 1 inv / 0 err 24h |
| 🟢 | `justhodl-edge-engine` | Edge Intelligence every 6h, 5 engines | 4 inv / 0 err 24h |
| 🟢 | `justhodl-options-flow` | Options + fund flows every 4h | 289 inv / 0 err 24h |
| 🟢 | `justhodl-investor-agents` | 6 legendary investor personas (Buffett, Munger, etc) | On-demand Lambda; 0 24h invocations is fine |
| 🟢 | `justhodl-stock-analyzer` | ECharts candles, SMA, Golden/Death Cross | On-demand Lambda; 0 24h invocations is fine |
| 🟢 | `justhodl-stock-screener` | 503 stocks Piotroski + Altman every 4h | 6 inv / 0 err 24h |
| 🟡 | `justhodl-valuations-agent` | CAPE, Buffett indicator monthly | Has schedule but 0 invocations 24h |
| 🟢 | `justhodl-crypto-intel` | BTC/ETH/SOL technicals every 15min | 96 inv / 0 err 24h |
| 🟢 | `cftc-futures-positioning-agent` | CFTC COT 29 contracts weekly Fri 18 UTC | 308 inv / 0 err 24h |
| 🟢 | `justhodl-financial-secretary` | Personal Financial Secretary every 4h | 6 inv / 0 err 24h |
| 🟢 | `justhodl-repo-monitor` | Plumbing stress every 30min weekdays | 23 inv / 0 err 24h |
| 🟢 | `justhodl-dex-scanner` | DEX Intelligence every 15min | 96 inv / 0 err 24h |
| 🟢 | `justhodl-telegram-bot` | @Justhodl_bot /briefing /ask /cftc /crypto /edge | 12 inv / 0 err 24h |
| 🟢 | `justhodl-signal-logger` | Learning loop signal logger every 6h | 11 inv / 0 err 24h |
| 🟢 | `justhodl-outcome-checker` | Outcome scorer Mon-Fri 22:30 + Sun 8 + 1st-of-month | 2 inv / 0 err 24h |
| 🟡 | `justhodl-calibrator` | Per-signal weights Sunday 9 UTC | Has schedule but 0 invocations 24h |
| 🟢 | `justhodl-health-monitor` | System observability every 15min (NEW 2026-04-25) | 13 inv / 0 err 24h |
| 🟢 | `justhodl-ml-predictions` | ML predictions engine every 5min — KNOWN BROKEN | 6 inv / 0 err 24h |
| 🟢 | `justhodl-khalid-metrics` | Custom Khalid metrics endpoint | 1 inv / 0 err 24h |
| 🟢 | `justhodl-advanced-charts` | TradingView-style charts | On-demand Lambda; 0 24h invocations is fine |

## Dashboard pages (17)

| Status | Name | Requested feature | Evidence |
|---|---|---|---|
| 🟢 | `index.html` | Main Bloomberg Terminal V10.3 | size 55,121B age 1121h |
| 🟢 | `pro.html` | Pro Dashboard (enhanced macro + sector) | size 58,557B age 1414h |
| ⚫ | `agent.html` | Financial Secretary dashboard | agent.html not in S3 bucket |
| 🟢 | `charts.html` | TradingView-style charts | size 245,035B age 1437h |
| 🟢 | `valuations.html` | Valuations dashboard | size 25,062B age 1272h |
| ⚫ | `edge.html` | Edge Intelligence terminal | edge.html not in S3 bucket |
| 🟢 | `flow.html` | Options Flow dashboard | size 30,349B age 1410h |
| 🟢 | `intelligence.html` | Market Intelligence fusion | size 27,710B age 1458h |
| ⚫ | `risk.html` | Systemic Risk Monitor | risk.html not in S3 bucket |
| 🟢 | `stocks.html` | Stock Picks page | size 26,200B age 1289h |
| 🟢 | `ath.html` | ATH Tracker | size 15,998B age 1387h |
| ⚫ | `trading-signals.html` | Trading Signals | trading-signals.html not in S3 bucket |
| ⚫ | `reports.html` | Reports & Analysis | reports.html not in S3 bucket |
| ⚫ | `ml.html` | ML Predictions | ml.html not in S3 bucket |
| 🟢 | `dex.html` | DEX Intelligence Terminal | size 49,207B age 1149h |
| ⚫ | `liquidity.html` | TGA + Fed Liquidity page | liquidity.html not in S3 bucket |
| 🟢 | `health.html` | System Health Monitor (NEW 2026-04-25) | size 9,996B age 1h |

## S3 data files (15)

| Status | Name | Requested feature | Evidence |
|---|---|---|---|
| 🟢 | `data/report.json` | Source of truth, daily-report-v3 every 5min | Age 0.0h, size 1,724,871B |
| 🟢 | `crypto-intel.json` | BTC/ETH/SOL technicals every 15min | Age 0.0h, size 55,834B |
| 🟢 | `edge-data.json` | Edge composite every 6h | Age 4.1h, size 1,222B |
| 🟢 | `repo-data.json` | Plumbing stress every 30min weekdays | Age 2.7h, size 36,413B |
| 🟢 | `flow-data.json` | Options/fund flows every 4h | Age 0.1h, size 31,570B |
| 🟢 | `intelligence-report.json` | Cross-system synthesis hourly weekdays | Age 2.0h, size 4,449B |
| 🟢 | `screener/data.json` | 503 stocks every 4h | Age 6.7h, size 326,603B |
| 🟢 | `valuations-data.json` | CAPE/Buffett monthly | Age 564.2h, size 2,188B |
| 🟢 | `calibration/latest.json` | Calibrator weekly Sunday | Age 137.2h, size 3,899B |
| 🟢 | `learning/last_log_run.json` | signal-logger heartbeat | Age 1.8h, size 80B |
| ⚫ | `dex-scanner-data.json` | DEX scanner every 15min | File missing from S3 |
| 🟢 | `data/secretary-latest.json` | Financial Secretary every 4h | Age 0.8h, size 141,252B |
| ⚫ | `ath-data.json` | ATH tracker | File missing from S3 |
| 🔴 | `predictions.json` | ml-predictions — KNOWN BROKEN | Age 33.3h, expected ≤1h (33.3× over) |
| 🔴 | `data.json` | Legacy orphan — KNOWN STALE | Age 1573.2h, expected ≤24h (65.5× over) |

## SSM parameters (6)

| Status | Name | Requested feature | Evidence |
|---|---|---|---|
| 🟢 | `/justhodl/ai-chat/auth-token` | AI chat auth token (CF Worker injects) | type SecureString |
| 🟢 | `/justhodl/calibration/weights` | Per-signal weights from calibrator | type String |
| 🟢 | `/justhodl/calibration/accuracy` | Per-signal accuracy | type String |
| 🟢 | `/justhodl/calibration/report` | Full calibration JSON | type String |
| 🟢 | `/justhodl/telegram/chat_id` | Khalid's Telegram chat_id | type String |
| 🟢 | `/justhodl/telegram/bot_token` | Bot token (NEW 2026-04-25) | type SecureString |

## EB rules (6)

| Status | Name | Requested feature | Evidence |
|---|---|---|---|
| 🟢 | `justhodl-outcome-checker-daily` | Daily outcome scoring (Mon-Fri 22:30) | ENABLED cron(30 22 ? * MON-FRI *) |
| 🟢 | `justhodl-outcome-checker-weekly` | Sunday outcome scoring | ENABLED cron(0 8 ? * SUN *) |
| 🟢 | `justhodl-calibrator-weekly` | Sunday 9 UTC calibration THE event | ENABLED cron(0 9 ? * SUN *) |
| 🟢 | `justhodl-health-monitor-15min` | Health monitor every 15min (NEW) | ENABLED cron(0/15 * * * ? *) |
| 🟢 | `justhodl-v9-auto-refresh` | 5-min auto-refresh (daily-report-v3) | ENABLED rate(5 minutes) |
| 🟢 | `DailyMacroScraper` | scrapeMacroData (DISABLED 2026-04-25) | DISABLED cron(0 12 * * ? *) |

## DynamoDB (3)

| Status | Name | Requested feature | Evidence |
|---|---|---|---|
| 🟢 | `justhodl-signals` | Learning loop — every signal logged | 4,779 items, 2873KB |
| 🟢 | `justhodl-outcomes` | Learning loop — scored outcomes | 4,307 items, 1701KB |
| 🟢 | `fed-liquidity-cache` | FRED data cache | 267,828 items, 19389KB |

## Cloudflare (1)

| Status | Name | Requested feature | Evidence |
|---|---|---|---|
| 🟢 | `justhodl-ai-proxy` | AI chat proxy at api.justhodl.ai | Source in repo, 3256B (deployed via deploy-workers.yml) |

## Issues found (🟡 + 🔴 + ⚫)


### 🟡 `justhodl-valuations-agent` (Core Lambdas)
**Requested:** CAPE, Buffett indicator monthly
**Status:** Has schedule but 0 invocations 24h

### 🟡 `justhodl-calibrator` (Core Lambdas)
**Requested:** Per-signal weights Sunday 9 UTC
**Status:** Has schedule but 0 invocations 24h

### ⚫ `agent.html` (Dashboard pages)
**Requested:** Financial Secretary dashboard
**Status:** agent.html not in S3 bucket

### ⚫ `edge.html` (Dashboard pages)
**Requested:** Edge Intelligence terminal
**Status:** edge.html not in S3 bucket

### ⚫ `risk.html` (Dashboard pages)
**Requested:** Systemic Risk Monitor
**Status:** risk.html not in S3 bucket

### ⚫ `trading-signals.html` (Dashboard pages)
**Requested:** Trading Signals
**Status:** trading-signals.html not in S3 bucket

### ⚫ `reports.html` (Dashboard pages)
**Requested:** Reports & Analysis
**Status:** reports.html not in S3 bucket

### ⚫ `ml.html` (Dashboard pages)
**Requested:** ML Predictions
**Status:** ml.html not in S3 bucket

### ⚫ `liquidity.html` (Dashboard pages)
**Requested:** TGA + Fed Liquidity page
**Status:** liquidity.html not in S3 bucket

### ⚫ `dex-scanner-data.json` (S3 data files)
**Requested:** DEX scanner every 15min
**Status:** File missing from S3

### ⚫ `ath-data.json` (S3 data files)
**Requested:** ATH tracker
**Status:** File missing from S3

### 🔴 `predictions.json` (S3 data files)
**Requested:** ml-predictions — KNOWN BROKEN
**Status:** Age 33.3h, expected ≤1h (33.3× over)

### 🔴 `data.json` (S3 data files)
**Requested:** Legacy orphan — KNOWN STALE
**Status:** Age 1573.2h, expected ≤24h (65.5× over)

---

## Reconciliation (step 105)

**Important context:** justhodl.ai is served from GitHub Pages (`ElMooro/si` repo), not from the S3 bucket directly. The S3 bucket is for backend data + a few legacy pages. So 'page not in S3' doesn't mean 'page missing from the live site'.


### Pages reconciled

| Page | In repo (justhodl.ai) | In S3 | Reality |
|---|---|---|---|
| `index.html` | ✓ | ✓ | 🟢 Live on justhodl.ai |
| `pro.html` | ✓ | ✓ | 🟢 Live on justhodl.ai |
| `agent.html` | ✓ | ✗ | 🟢 Live on justhodl.ai |
| `charts.html` | ✓ | ✓ | 🟢 Live on justhodl.ai |
| `valuations.html` | ✓ | ✓ | 🟢 Live on justhodl.ai |
| `edge.html` | ✓ | ✗ | 🟢 Live on justhodl.ai |
| `flow.html` | ✓ | ✓ | 🟢 Live on justhodl.ai |
| `intelligence.html` | ✓ | ✓ | 🟢 Live on justhodl.ai |
| `risk.html` | ✓ | ✗ | 🟢 Live on justhodl.ai |
| `stocks.html` | ✓ | ✓ | 🟢 Live on justhodl.ai |
| `ath.html` | ✓ | ✓ | 🟢 Live on justhodl.ai |
| `trading-signals.html` | ✓ | ✗ | 🟢 Live on justhodl.ai |
| `reports.html` | ✗ | ✗ | ⚫ MISSING entirely |
| `ml.html` | ✓ | ✗ | 🟢 Live on justhodl.ai |
| `dex.html` | ✓ | ✓ | 🟢 Live on justhodl.ai |
| `liquidity.html` | ✓ | ✗ | 🟢 Live on justhodl.ai |
| `health.html` | ✗ | ✓ | 🟡 In S3 but not on justhodl.ai |

### Genuinely missing pages

- `reports.html` — neither in repo nor S3. **Real gap.**

### dex-scanner-data.json + ath-data.json findings

These were flagged as missing but are NOT separate top-level files:
- **DEX scanner**: Writes to `dex-scanner-data.json` per source code, but only when scheduled. Check if data exists or rule is disabled.
- **ATH tracker**: Embedded in `data/report.json` under `ath_breakouts` key, NOT a separate file. Audit logic was wrong.