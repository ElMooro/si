# JustHodl.AI — System Architecture (canonical, 2026-04-25)

**Generated:** 2026-04-25T00:37:48.022299+00:00  
**Source:** [aws/ops/audit/inventory_2026-04-25.json](inventory_2026-04-25.json)  
**Account:** AWS 857687956942 (us-east-1) + Cloudflare 2e120c8358c6c85dcaba07eb16947817


---

## At a glance

- **95 Lambda functions** (50 in repo, 45 not yet pulled into version control)
- **5000 S3 objects** in `justhodl-dashboard-live` (capped at 5000 — actual total is higher)
- **25 DynamoDB tables** (only 3 actively used: `justhodl-signals`, `justhodl-outcomes`, `fed-liquidity-cache`)
- **98 EventBridge rules** (90 enabled, 8 disabled)
- **5 SSM parameters** under `/justhodl/`
- **1 Cloudflare Worker:** `justhodl-ai-proxy` at `api.justhodl.ai` → ai-chat Lambda

## Critical path: how a signal becomes a calibrated weight

```
External APIs (FRED, Polygon, FMP, CoinGecko, etc.)
        │
        ▼
  ┌─────────────────────────────────┐
  │  data collector Lambdas (~30)   │  ← fetch + write to S3
  └─────────────────────────────────┘
        │
        ▼
  ┌──────────────────────────────────────────────────────┐
  │  S3 (justhodl-dashboard-live)                        │
  │    data/report.json   ← daily-report-v3 every 5 min  │
  │    crypto-intel.json  ← crypto-intel every 6h        │
  │    edge-data.json     ← edge-engine every 6h         │
  │    repo-data.json     ← repo-monitor                 │
  │    flow-data.json     ← options-flow                 │
  │    valuations-data.json ← valuations-agent monthly   │
  │    screener/data.json ← stock-screener every 4h      │
  │    intelligence-report.json ← justhodl-intelligence  │
  └──────────────────────────────────────────────────────┘
        │
        ▼
  ┌─────────────────────────────────────────┐
  │  justhodl-signal-logger (every 6h)      │  ← logs SIGNALS to DynamoDB
  │  → justhodl-signals (4,579 items)       │     with baseline_price + schema_v2
  └─────────────────────────────────────────┘
        │
        ▼
  ┌─────────────────────────────────────────┐
  │  justhodl-outcome-checker               │  ← evaluates signals after windows
  │    Mon-Fri 22:30 UTC                    │     elapse, writes outcomes
  │    Sun 8:00 UTC                         │
  │    1st of month 8:00 UTC                │
  │  → justhodl-outcomes (738 items)        │
  └─────────────────────────────────────────┘
        │
        ▼
  ┌─────────────────────────────────────────┐
  │  justhodl-calibrator (Sunday 9:00 UTC)  │  ← computes per-signal accuracy,
  │  → SSM /justhodl/calibration/weights    │     writes weights for 24 signals
  │  → SSM /justhodl/calibration/accuracy   │
  │  → S3 calibration/latest.json           │
  └─────────────────────────────────────────┘
```

## Lambda inventory by purpose

### Core Pipeline (10)

_Core data pipeline — these produce the S3 data that everything else reads_

#### `justhodl-crypto-intel`
- runtime=python3.12, mem=1024MB, timeout=180s
- **Schedules:** rate(15 minutes)
- LOC: 3729
- **Writes S3:** `crypto-intel.json`
- **External APIs:** `api.anthropic.com`, `api.binance.com`, `api.coingecko.com`, `api.etherscan.io`, `api1.binance.com`, `api2.binance.com`…
- env: ANTHROPIC_API_KEY, CMC_API_KEY, S3_BUCKET

#### `justhodl-daily-report-v3`
- runtime=python3.12, mem=1024MB, timeout=900s
- **Schedules:** rate(5 minutes), cron(0 23 ? * MON-FRI *), cron(0 13 ? * MON-FRI *)
- LOC: 1791
- **Reads S3:** `data/ath.json`
- **Writes S3:** `data/archive/report_{ts}.json`, `data/ath.json`, `data/report.json`
- **External APIs:** `api.coingecko.com`, `api.polygon.io`, `api.stlouisfed.org`, `news.google.com`, `newsapi.org`
- env: EMAIL_FROM, EMAIL_TO, FRED_API_KEY, NEWS_API_KEY, POLYGON_API_KEY, S3_BUCKET

#### `justhodl-edge-engine`
- runtime=python3.12, mem=512MB, timeout=120s
- **Schedules:** rate(6 hours)
- LOC: 189
- **Writes S3:** `edge-data.json`
- **External APIs:** `api.polygon.io`, `api.stlouisfed.org`

#### `justhodl-intelligence`
- runtime=python3.12, mem=256MB, timeout=120s
- **Schedules:** cron(10 12 * * ? *), cron(5 12-23 ? * MON-FRI *)
- LOC: 874
- **Writes S3:** `archive/intelligence/{dk}.json`, `intelligence-report.json`
- env: S3_BUCKET

#### `justhodl-investor-agents`
- runtime=python3.11, mem=512MB, timeout=120s
- LOC: 196
- **Reads S3:** `data/report.json`
- **Writes S3:** `investor-analysis/`
- **External APIs:** `api.anthropic.com`, `financialmodelingprep.com`
- env: ANTHROPIC_KEY, FMP_KEY

#### `justhodl-morning-intelligence`
- runtime=python3.12, mem=256MB, timeout=120s
- **Schedules:** cron(0 13 * * ? *)
- LOC: 358
- **Reads S3:** `learning/last_log_run.json`
- **Writes S3:** `learning/morning_run_log.json`
- **DynamoDB:** `justhodl-outcomes`
- **External APIs:** `api.anthropic.com`, `api.telegram.org`
- env: ANTHROPIC_KEY

#### `justhodl-options-flow`
- runtime=python3.11, mem=1024MB, timeout=300s
- **Schedules:** rate(5 minutes)
- LOC: 4694
- **Writes S3:** `flow-data.json`
- **External APIs:** `api.polygon.io`, `api.stlouisfed.org`
- env: AV_KEY, FRED_KEY, NEWS_KEY, POLYGON_KEY

#### `justhodl-stock-analyzer`
- runtime=python3.12, mem=512MB, timeout=120s
- LOC: 469
- **Reads S3:** `data/report.json`
- **Writes S3:** `stock-analysis/{ticker}.json`
- **External APIs:** `api.polygon.io`, `financialmodelingprep.com`

#### `justhodl-stock-screener`
- runtime=python3.11, mem=1024MB, timeout=600s
- **Schedules:** rate(4 hours)
- LOC: 248
- **External APIs:** `financialmodelingprep.com`

#### `justhodl-valuations-agent`
- runtime=python3.11, mem=512MB, timeout=120s
- **Schedules:** cron(0 14 1 * ? *)
- LOC: 347
- **Writes S3:** `valuations-data.json`
- **External APIs:** `api.polygon.io`, `api.stlouisfed.org`, `pro-api.coinmarketcap.com`
- env: AV_KEY, FRED_KEY, POLYGON_KEY, S3_BUCKET

### Data Collectors (34)

_External API fetchers — write to S3 on schedule_

#### `alphavantage-market-agent`
- runtime=python3.9, mem=256MB, timeout=60s
- **Schedules:** cron(*/15 13-21 ? * MON-FRI *)
- ⚠ Source not in repo — pull via ops script if needed
- env: ALPHAVANTAGE_API_KEY

#### `bea-economic-agent`
- runtime=python3.9, mem=256MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed
- env: BEA_API_KEY

#### `benzinga-news-agent`
- runtime=python3.12, mem=512MB, timeout=120s
- ⚠ Source not in repo — pull via ops script if needed
- env: BENZINGA_API_KEY

#### `bls-labor-agent`
- runtime=python3.9, mem=256MB, timeout=60s
- **Schedules:** cron(30 13 * * ? *)
- ⚠ Source not in repo — pull via ops script if needed
- env: BLS_API_KEY

#### `bond-indices-agent`
- runtime=python3.9, mem=512MB, timeout=60s
- **Schedules:** rate(1 hour)
- ⚠ Source not in repo — pull via ops script if needed
- env: FRED_API_KEY

#### `census-economic-agent`
- runtime=python3.9, mem=256MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed
- env: CENSUS_API_KEY

#### `chatgpt-agent-api`
- runtime=python3.11, mem=512MB, timeout=30s
- LOC: 147
- env: FROM_EMAIL, REPORTS_BUCKET, TO_EMAILS

#### `coinmarketcap-agent`
- runtime=python3.9, mem=3008MB, timeout=300s
- ⚠ Source not in repo — pull via ops script if needed

#### `dollar-strength-agent`
- runtime=python3.9, mem=512MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed
- env: FRED_API_KEY

#### `ecb-auto-updater`
- runtime=python3.9, mem=256MB, timeout=60s
- **Schedules:** cron(0 6 ? * MON *)
- ⚠ Source not in repo — pull via ops script if needed

#### `ecb-data-daily-updater`
- runtime=python3.9, mem=256MB, timeout=60s
- **Schedules:** cron(0 6 * * ? *)
- ⚠ Source not in repo — pull via ops script if needed

#### `eia-energy-agent`
- runtime=python3.12, mem=512MB, timeout=120s
- ⚠ Source not in repo — pull via ops script if needed
- env: EIA_API_KEY

#### `enhanced-repo-agent`
- runtime=python3.9, mem=512MB, timeout=60s
- **Schedules:** rate(15 minutes)
- ⚠ Source not in repo — pull via ops script if needed

#### `fmp-fundamentals-agent`
- runtime=python3.12, mem=512MB, timeout=120s
- ⚠ Source not in repo — pull via ops script if needed
- env: FMP_API_KEY

#### `fmp-stock-picks-agent`
- runtime=python3.12, mem=512MB, timeout=900s
- **Schedules:** cron(0 14,16,18,20 ? * MON-FRI *), cron(0 12 ? * MON-FRI *), cron(0 12 ? * MON-FRI *)
- LOC: 441
- **Writes S3:** `reports/dlb_{datetime.utcnow().strftime(`
- **External APIs:** `api.justhodl.ai`, `api.stlouisfed.org`, `www.cfr.org`, `www.financialresearch.gov`, `www.worldgovernmentbonds.com`
- env: FMP_API_KEY, S3_BUCKET

#### `global-liquidity-agent-TEST`
- runtime=python3.11, mem=1024MB, timeout=300s
- ⚠ Source not in repo — pull via ops script if needed
- env: FRED_API_KEY, S3_BUCKET, SNS_TOPIC_ARN

#### `global-liquidity-agent-v2`
- runtime=python3.11, mem=1024MB, timeout=300s
- **Schedules:** cron(0 12 * * ? *), cron(0 13 * * ? *)
- ⚠ Source not in repo — pull via ops script if needed
- env: ENABLE_ML_PREDICTIONS, ENABLE_OPTIONS_FLOW, ENABLE_REAL_TIME_ALERTS, FRED_API_KEY, S3_BUCKET, SES_SENDER, SNS_TOPIC_ARN

#### `google-trends-agent`
- runtime=python3.11, mem=1024MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed

#### `justhodl-charts-agent`
- runtime=python3.9, mem=512MB, timeout=30s
- LOC: 168
- **External APIs:** `api.justhodl.ai`, `unpkg.com`

#### `justhodl-data-collector`
- runtime=python3.9, mem=128MB, timeout=15s
- **Schedules:** rate(1 hour)
- ⚠ Source not in repo — pull via ops script if needed

#### `justhodl-liquidity-agent`
- runtime=python3.12, mem=512MB, timeout=120s
- **Schedules:** cron(30 12 * * ? *)
- ⚠ Source not in repo — pull via ops script if needed
- env: FRED_API_KEY, S3_BUCKET

#### `manufacturing-global-agent`
- runtime=python3.9, mem=512MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed
- env: FRED_API_KEY

#### `multi-agent-orchestrator`
- runtime=python3.11, mem=512MB, timeout=60s
- **Schedules:** cron(*/5 9-16 ? * MON-FRI *)
- ⚠ Source not in repo — pull via ops script if needed
- env: LIQUIDITY_URL, POLYGON_API, TREASURY_API, TREASURY_LAMBDA

#### `nasdaq-datalink-agent`
- runtime=python3.12, mem=512MB, timeout=120s
- LOC: 104
- **External APIs:** `data.nasdaq.com`
- env: NASDAQ_API_KEY

#### `news-sentiment-agent`
- runtime=python3.9, mem=512MB, timeout=30s
- **Schedules:** rate(30 minutes)
- LOC: 7
- env: NEWSAPI_KEY

#### `nyfed-financial-stability-fetcher`
- runtime=python3.9, mem=256MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed

#### `nyfed-primary-dealer-fetcher`
- runtime=python3.9, mem=256MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed

#### `securities-banking-agent`
- runtime=python3.9, mem=512MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed
- env: FRED_API_KEY

#### `testEnhancedScraper`
- runtime=python3.11, mem=1024MB, timeout=300s
- ⚠ Source not in repo — pull via ops script if needed
- env: S3_BUCKET

#### `treasury-auto-updater`
- runtime=python3.9, mem=128MB, timeout=3s
- **Schedules:** cron(0 10 ? * MON *), cron(0 10 ? * THU *)
- ⚠ Source not in repo — pull via ops script if needed

#### `ultimate-multi-agent`
- runtime=python3.11, mem=1024MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed
- env: AGENT_URLS, FRED_API_KEY, POLYGON_API_KEY

#### `universal-agent-gateway`
- runtime=python3.9, mem=1024MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed

#### `volatility-monitor-agent`
- runtime=python3.9, mem=512MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed
- env: FRED_API_KEY

#### `xccy-basis-agent`
- runtime=python3.9, mem=256MB, timeout=30s
- **Schedules:** rate(30 minutes)
- ⚠ Source not in repo — pull via ops script if needed
- env: FRED_API_KEY

### Intelligence Agents (9)

_Multi-source aggregators that produce derived insights_

#### `FinancialIntelligence-Backend`
- runtime=python3.11, mem=512MB, timeout=60s
- LOC: 197
- **External APIs:** `api.polygon.io`, `api.stlouisfed.org`

#### `aiapi-market-analyzer`
- runtime=python3.13, mem=10240MB, timeout=900s
- **Schedules:** cron(0 7 * * ? *), rate(1 hour), rate(15 minutes), cron(0 8 ? * SUN *)
- ⚠ Source not in repo — pull via ops script if needed

#### `autonomous-ai-processor`
- runtime=python3.11, mem=1024MB, timeout=300s
- **Schedules:** rate(5 minutes)
- ⚠ Source not in repo — pull via ops script if needed

#### `daily-liquidity-report`
- runtime=python3.11, mem=2048MB, timeout=300s
- **Schedules:** cron(45 12 * * ? *)
- ⚠ Source not in repo — pull via ops script if needed

#### `justhodl-daily-macro-report`
- runtime=python3.11, mem=128MB, timeout=3s
- **Schedules:** cron(0 12 * * ? *)
- ⚠ Source not in repo — pull via ops script if needed

#### `justhodl-email-reports`
- runtime=python3.12, mem=1024MB, timeout=120s
- **Schedules:** cron(0 13 * * ? *)
- ⚠ Source not in repo — pull via ops script if needed

#### `justhodl-email-reports-v2`
- runtime=python3.11, mem=1024MB, timeout=300s
- **Schedules:** cron(0 12 * * ? *)
- ⚠ Source not in repo — pull via ops script if needed
- env: FRED_API_KEY, S3_BUCKET, SES_SENDER

#### `macro-financial-intelligence`
- runtime=python3.11, mem=512MB, timeout=900s
- **Schedules:** cron(0 14 * * ? *)
- LOC: 43
- **Reads S3:** `latest_brief.json`
- **Writes S3:** `latest_brief.json`
- **External APIs:** `api.stlouisfed.org`
- env: FRED_API_KEY, S3_BUCKET

#### `permanent-market-intelligence`
- runtime=python3.9, mem=512MB, timeout=60s
- **Schedules:** cron(0 13 * * ? *)
- LOC: 341

### User Facing (6)

_Lambdas with Function URLs, called by the browser or Telegram bot_

#### `cftc-futures-positioning-agent`
- runtime=python3.11, mem=512MB, timeout=120s
- **Schedules:** cron(0 18 ? * FRI *)
- LOC: 430
- **External APIs:** `api.polygon.io`, `publicreporting.cftc.gov`
- env: POLYGON_API_KEY

#### `justhodl-advanced-charts`
- runtime=python3.9, mem=1024MB, timeout=30s
- LOC: 168
- **External APIs:** `api.justhodl.ai`, `unpkg.com`

#### `justhodl-ai-chat`
- runtime=python3.12, mem=512MB, timeout=60s
- LOC: 278
- **External APIs:** `api.anthropic.com`, `api.coingecko.com`, `api.polygon.io`, `justhodl.ai`, `www.justhodl.ai`
- env: ANTHROPIC_API_KEY, S3_BUCKET

#### `justhodl-bloomberg-v8`
- runtime=python3.12, mem=2048MB, timeout=300s
- **Schedules:** rate(5 minutes)
- LOC: 599
- **Writes S3:** `archive/{et.strftime(`, `data/report.json`
- **External APIs:** `api.polygon.io`, `api.stlouisfed.org`, `newsapi.org`, `pro-api.coinmarketcap.com`
- env: ALPHAVANTAGE_API_KEY, ANTHROPIC_API_KEY, CMC_API_KEY, FRED_API_KEY, NEWS_API_KEY, POLYGON_API_KEY, S3_BUCKET

#### `justhodl-chat-api`
- runtime=python3.12, mem=512MB, timeout=30s
- LOC: 133
- **Reads S3:** `data/report.json`
- **External APIs:** `api.anthropic.com`
- env: ANTHROPIC_API_KEY

#### `justhodl-khalid-metrics`
- runtime=python3.12, mem=512MB, timeout=240s
- **Schedules:** cron(0 11 * * ? *)
- LOC: 369
- **Reads S3:** `data/khalid-analysis.json`, `data/khalid-config.json`, `data/khalid-metrics.json`
- **Writes S3:** `data/khalid-analysis.json`, `data/khalid-config.json`, `data/khalid-metrics.json`
- **External APIs:** `api.anthropic.com`, `api.polygon.io`, `api.stlouisfed.org`
- env: ANTHROPIC_API_KEY, FRED_API_KEY, POLYGON_API_KEY, S3_BUCKET

### Learning Loop (3)

_Calibration system (Week 1 fix shipped 2026-04-24)_

#### `justhodl-calibrator`
- runtime=python3.12, mem=256MB, timeout=120s
- **Schedules:** cron(0 9 ? * SUN *)
- LOC: 393
- **Writes S3:** `calibration/history/{now.strftime(`, `calibration/latest.json`

#### `justhodl-outcome-checker`
- runtime=python3.12, mem=256MB, timeout=300s
- **Schedules:** cron(30 22 ? * MON-FRI *), cron(0 8 1 * ? *), cron(0 8 ? * SUN *)
- LOC: 365
- **Reads S3:** `data/report.json`
- **External APIs:** `api.coingecko.com`, `api.polygon.io`, `financialmodelingprep.com`

#### `justhodl-signal-logger`
- runtime=python3.12, mem=256MB, timeout=120s
- **Schedules:** rate(6 hours)
- LOC: 299
- **Reads S3:** `crypto-intel.json`, `data/report.json`, `edge-data.json`, `intelligence-report.json`, `repo-data.json`, `screener/data.json`, `valuations-data.json`
- **Writes S3:** `learning/last_log_run.json`
- **External APIs:** `api.coingecko.com`, `api.polygon.io`, `financialmodelingprep.com`

### Telegram Bot (1)

_Telegram integration_

#### `justhodl-telegram-bot`
- runtime=python3.12, mem=256MB, timeout=90s
- **Schedules:** rate(2 hours)
- LOC: 824
- **Reads S3:** `telegram/alert_state.json`
- **Writes S3:** `telegram/alert_state.json`
- **External APIs:** `api.anthropic.com`, `api.telegram.org`, `financialmodelingprep.com`, `quickchart.io`
- env: ALLOWED_CHAT_IDS, ANTHROPIC_API_KEY, S3_BUCKET, TELEGRAM_TOKEN

### Deprecated Or Unclear (28)

_Purpose unclear from name — investigate or retire_

#### `MLPredictor`
- runtime=python3.10, mem=1024MB, timeout=30s
- **Schedules:** cron(15 12 * * ? *)
- LOC: 65

#### `alphavantage-technical-analysis`
- runtime=python3.9, mem=512MB, timeout=30s
- LOC: 47

#### `bls-employment-api-v2`
- runtime=nodejs18.x, mem=512MB, timeout=900s
- **Schedules:** cron(0 22 ? * FRI *), cron(0 22 ? * TUE *)
- ⚠ Source not in repo — pull via ops script if needed
- env: BLS_API_KEY

#### `createEnhancedIndex`
- runtime=python3.11, mem=1024MB, timeout=300s
- ⚠ Source not in repo — pull via ops script if needed
- env: S3_BUCKET

#### `createUniversalIndex`
- runtime=python3.11, mem=512MB, timeout=300s
- ⚠ Source not in repo — pull via ops script if needed

#### `ecb`
- runtime=python3.9, mem=512MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed
- env: BUCKET, CACHE_DURATION, KEY

#### `economyapi`
- runtime=python3.9, mem=256MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed

#### `fedliquidityapi`
- runtime=python3.9, mem=256MB, timeout=30s
- **Schedules:** cron(0 13 ? * MON,WED,FRI *)
- LOC: 550
- **External APIs:** `api.stlouisfed.org`

#### `fedliquidityapi-test`
- runtime=python3.12, mem=512MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed

#### `fred-ice-bofa-api`
- runtime=python3.9, mem=1024MB, timeout=60s
- **Schedules:** cron(0 9 ? * 1,3,5 *)
- ⚠ Source not in repo — pull via ops script if needed
- env: CACHE_DURATION

#### `justhodl-cache-layer`
- runtime=python3.9, mem=512MB, timeout=35s
- ⚠ Source not in repo — pull via ops script if needed

#### `justhodl-crypto-enricher`
- runtime=python3.12, mem=256MB, timeout=120s
- **Schedules:** cron(15 6 * * ? *)
- LOC: 283
- **External APIs:** `api.blocknative.com`, `api.gasprice.io`, `fapi.binance.com`, `open-api.coinglass.com`
- env: CMC_API_KEY, S3_BUCKET

#### `justhodl-dex-scanner`
- runtime=python3.12, mem=256MB, timeout=120s
- **Schedules:** rate(15 minutes)
- LOC: 26
- **Reads S3:** `dex.html`
- **External APIs:** `api.github.com`
- env: TOKEN

#### `justhodl-ecb-proxy`
- runtime=python3.12, mem=512MB, timeout=120s
- LOC: 207
- **External APIs:** `api.stlouisfed.org`

#### `justhodl-financial-secretary`
- runtime=python3.12, mem=1024MB, timeout=300s
- **Schedules:** rate(4 hours)
- LOC: 1453
- **Reads S3:** `crypto-intel.json`, `data/fred-cache-secretary.json`, `data/fred-cache.json`, `data/report.json`, `data/secretary-latest.json`, `flow-data.json`
- **Writes S3:** `data/fred-cache-secretary.json`, `data/secretary-history/{now.strftime(`, `data/secretary-latest.json`
- **External APIs:** `api.anthropic.com`, `api.polygon.io`, `api.stlouisfed.org`, `justhodl.ai`, `newsapi.org`, `pro-api.coinmarketcap.com`
- env: ALPHAVANTAGE_KEY, ANTHROPIC_API_KEY, CMC_API_KEY, EMAIL_FROM, EMAIL_TO, FRED_API_KEY, NEWS_API_KEY, POLYGON_API_KEY…

#### `justhodl-fred-proxy`
- runtime=python3.12, mem=256MB, timeout=30s
- LOC: 41
- **External APIs:** `api.stlouisfed.org`

#### `justhodl-ml-predictions`
- runtime=python3.12, mem=512MB, timeout=120s
- **Schedules:** rate(4 hours)
- LOC: 320
- **Writes S3:** `predictions.json`
- **External APIs:** `api.justhodl.ai`

#### `justhodl-news-sentiment`
- runtime=python3.11, mem=512MB, timeout=600s
- **Schedules:** cron(15 6 * * ? *)
- LOC: 227
- **External APIs:** `api.anthropic.com`, `financialmodelingprep.com`, `newsapi.org`
- env: ANTHROPIC_KEY

#### `justhodl-repo-monitor`
- runtime=python3.12, mem=512MB, timeout=300s
- **Schedules:** cron(0/30 13-23 ? * MON-FRI *), cron(0 12 * * ? *)
- LOC: 472
- **Writes S3:** `archive/repo/{dk}.json`, `repo-data.json`
- **External APIs:** `api.stlouisfed.org`, `markets.newyorkfed.org`
- env: FRED_API_KEY, S3_BUCKET

#### `justhodl-treasury-proxy`
- runtime=python3.12, mem=256MB, timeout=30s
- LOC: 133
- **External APIs:** `api.fiscaldata.treasury.gov`

#### `justhodl-ultimate-orchestrator`
- runtime=python3.9, mem=1024MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed

#### `justhodl-ultimate-trading`
- runtime=python3.9, mem=1024MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed

#### `macro-financial-report-viewer`
- runtime=python3.11, mem=256MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed

#### `macro-report-api`
- runtime=python3.9, mem=1024MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed

#### `nyfedapi-isolated`
- runtime=python3.9, mem=1024MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed

#### `ofrapi`
- runtime=python3.9, mem=512MB, timeout=120s
- **Schedules:** cron(0 14 * * ? *), cron(0 13 ? * MON *)
- ⚠ Source not in repo — pull via ops script if needed
- env: S3_BUCKET

#### `scrapeMacroData`
- runtime=python3.11, mem=3008MB, timeout=900s
- **Schedules:** cron(0 12 * * ? *)
- ⚠ Source not in repo — pull via ops script if needed
- env: BATCH_SIZE, CONFIG_FILE, FRED_API_KEY, INDEX_NAME, MAX_INDICATORS, OPENSEARCH_ENDPOINT, S3_BUCKET

#### `treasury-api`
- runtime=python3.9, mem=512MB, timeout=60s
- **Schedules:** cron(0 10 ? * MON,THU *)
- ⚠ Source not in repo — pull via ops script if needed
- env: S3_BUCKET, TREASURY_KEY

### Legacy Openbb (4)

_OpenBB-related Lambdas — appear unused now (consider retirement)_

#### `OpenBBS3DataProxy`
- runtime=python3.11, mem=512MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed

#### `openbb-system2-api`
- runtime=python3.9, mem=1024MB, timeout=30s
- LOC: 285
- **External APIs:** `api.stlouisfed.org`
- env: CACHE_RESET, FRED_API_KEY

#### `openbb-websocket-broadcast`
- runtime=python3.11, mem=512MB, timeout=60s
- ⚠ Source not in repo — pull via ops script if needed
- env: CONNECTION_TABLE, S3_BUCKET

#### `openbb-websocket-handler`
- runtime=python3.11, mem=256MB, timeout=30s
- ⚠ Source not in repo — pull via ops script if needed
- env: CONNECTION_TABLE, S3_BUCKET

## DynamoDB tables

| Table | Items | Size | Status |
|---|---:|---:|---|
| `fed-liquidity-cache` | 267,828 | 19389KB | 🟢 ACTIVE |
| `justhodl-signals` | 4,579 | 2211KB | 🟢 ACTIVE |
| `justhodl-outcomes` | 738 | 297KB | 🟢 ACTIVE |
| `openbb-historical-data` | 1 | 1KB | 🟢 ACTIVE |
| `ai-assistant-tasks` | 6 | 1KB | 🟢 ACTIVE |
| `openbb-trading-signals` | 2 | 0KB | 💤 empty |
| `liquidity-metrics-v2` | 1 | 0KB | 💤 empty |
| `APIKeys` | 0 | 0KB | 💤 empty |
| `MacroMetrics` | 0 | 0KB | 💤 empty |
| `OpenBBUsers` | 0 | 0KB | 💤 empty |
| `WebSocketConnections` | 0 | 0KB | 💤 empty |
| `agent-cache-table` | 0 | 0KB | 💤 empty |
| `aiapi-market-metadata` | 0 | 0KB | 💤 empty |
| `autonomous-ai-system-data` | 0 | 0KB | 💤 empty |
| `autonomous-ai-tasks` | 0 | 0KB | 💤 empty |
| `bls-data-857687956942-bls-minimal` | 0 | 0KB | 💤 empty |
| `chatgpt-agent-audit-log` | 0 | 0KB | 💤 empty |
| `chatgpt-agent-state` | 0 | 0KB | 💤 empty |
| `chatgpt-state` | 0 | 0KB | 💤 empty |
| `fed-liquidity-cache-v3` | 0 | 0KB | 💤 empty |
| `justhodl-historical` | 0 | 0KB | 💤 empty |
| `liquidity-indicators-v3` | 0 | 0KB | 💤 empty |
| `liquidity-reversals-v3` | 0 | 0KB | 💤 empty |
| `openbb-bls-data` | 0 | 0KB | 💤 empty |
| `openbb-bls-data-857687956942` | 0 | 0KB | 💤 empty |

**Active tables (real data):**
- `justhodl-signals` — every signal logged by signal-logger; 4,579 items
- `justhodl-outcomes` — scored outcomes from outcome-checker; 738 items
- `fed-liquidity-cache` — FRED data cache; 267k items, 19MB

Other 22 tables are empty/dead — most were created during prior architecture experiments.
Safe to delete after confirming no Lambda still references them.

## SSM parameters

| Name | Type | Purpose |
|---|---|---|
| `/justhodl/ai-chat/auth-token` | SecureString | Token for ai-chat Lambda; injected by CF Worker |
| `/justhodl/calibration/accuracy` | String | Per-signal accuracy stats (calibrator output) |
| `/justhodl/calibration/report` | String | Full calibration report JSON |
| `/justhodl/calibration/weights` | String | Per-signal weights (consumed by ranker, future) |
| `/justhodl/telegram/chat_id` | String | Khalid's Telegram chat ID for bot pushes |

## EventBridge schedules (90 enabled)

Most-frequent firing rules (cron/rate):

### Rate schedules (21)

- `rate(1 hour)` → `aiapi-market-analyzer`
- `rate(15 minutes)` → `aiapi-market-analyzer`
- `rate(1 hour)` → `aiapi-monitor`
- `rate(5 minutes)` → `autonomous-ai-processor`
- `rate(1 hour)` → `bond-indices-agent`
- `rate(15 minutes)` → `enhanced-repo-agent`
- `rate(5 minutes)` → `justhodl-bloomberg-v8`
- `rate(15 minutes)` → `justhodl-crypto-intel`
- `rate(5 minutes)` → `justhodl-daily-report-v3`
- `rate(1 hour)` → `justhodl-data-collector`
- `rate(15 minutes)` → `justhodl-dex-scanner`
- `rate(6 hours)` → `justhodl-edge-engine`
- `rate(4 hours)` → `justhodl-financial-secretary`
- `rate(4 hours)` → `justhodl-ml-predictions`
- `rate(5 minutes)` → `justhodl-options-flow`
- ... and 6 more

### Weekday schedules (13)

- `cron(*/15 13-21 ? * MON-FRI *)` → `alphavantage-market-agent`
- `cron(0 14,16,18,20 ? * MON-FRI *)` → `fmp-stock-picks-agent`
- `cron(0 12 ? * MON-FRI *)` → `fmp-stock-picks-agent`
- `cron(0 12 ? * MON-FRI *)` → `fmp-stock-picks-agent`
- `cron(0 23 ? * MON-FRI *)` → `justhodl-daily-report-v3`
- `cron(0 13 ? * MON-FRI *)` → `justhodl-daily-report-v3`
- `cron(5 12-23 ? * MON-FRI *)` → `justhodl-intelligence`
- `cron(30 22 ? * MON-FRI *)` → `justhodl-outcome-checker`
- `cron(0/30 13-23 ? * MON-FRI *)` → `justhodl-repo-monitor`
- `cron(*/5 9-16 ? * MON-FRI *)` → `multi-agent-orchestrator`
- `cron(0 13-21 ? * MON-FRI *)` → `openbb-ml_predictions`
- `cron(0/30 13-21 ? * MON-FRI *)` → `openbb-trading_signals`
- `cron(0/15 13-21 ? * MON-FRI *)` → `openbb-vix_alert`

### Weekly schedules (15)

- `cron(0 8 ? * SUN *)` → `aiapi-market-analyzer`
- `cron(0 22 ? * FRI *)` → `bls-employment-api-v2`
- `cron(0 18 ? * FRI *)` → `cftc-futures-positioning-agent`
- `cron(0 6 ? * MON *)` → `ecb-auto-updater`
- `cron(0 14 ? * MON *)` → `fed-liquidity-indicators`
- `cron(0 14 ? * MON *)` → `fedapi`
- `cron(0 14 ? * MON *)` → `fedliquidity`
- `cron(0 13 ? * MON,WED,FRI *)` → `fedliquidityapi`
- `cron(0 13 ? * MON,WED,FRI *)` → `fredapi`
- `cron(0 14 ? * MON *)` → `fredapi`
- `cron(0 9 ? * SUN *)` → `justhodl-calibrator`
- `cron(0 8 ? * SUN *)` → `justhodl-outcome-checker`
- `cron(0 13 ? * MON *)` → `ofrapi`
- `cron(0 10 ? * MON,THU *)` → `treasury-api`
- `cron(0 10 ? * MON *)` → `treasury-auto-updater`

### Daily schedules (24)

- `cron(0 7 * * ? *)` → `aiapi-market-analyzer`
- `cron(0 14 * * ? *)` → `aiapi-monitor`
- `cron(0 22 ? * TUE *)` → `bls-employment-api-v2`
- `cron(0 6 * * ? *)` → `ecb-data-daily-updater`
- `cron(0 14 ? * THU *)` → `fedapi`
- `cron(0 9 ? * 1,3,5 *)` → `fred-ice-bofa-api`
- `cron(0 12 * * ? *)` → `global-liquidity-agent-v2`
- `cron(0 13 * * ? *)` → `global-liquidity-agent-v2`
- `cron(0 12 * * ? *)` → `justhodl-daily-macro-report`
- `cron(0 13 * * ? *)` → `justhodl-email-reports`
- `cron(0 12 * * ? *)` → `justhodl-email-reports-v2`
- `cron(0 11 * * ? *)` → `justhodl-khalid-metrics`
- `cron(0 13 * * ? *)` → `justhodl-morning-intelligence`
- `cron(0 8 1 * ? *)` → `justhodl-outcome-checker`
- `cron(0 12 * * ? *)` → `justhodl-repo-monitor`
- ... and 9 more

### Other schedules (7)

- `cron(15 12 * * ? *)` → `MLPredictor`
- `cron(30 13 * * ? *)` → `bls-labor-agent`
- `cron(45 12 * * ? *)` → `daily-liquidity-report`
- `cron(15 6 * * ? *)` → `justhodl-crypto-enricher`
- `cron(10 12 * * ? *)` → `justhodl-intelligence`
- `cron(30 12 * * ? *)` → `justhodl-liquidity-agent`
- `cron(15 6 * * ? *)` → `justhodl-news-sentiment`

## S3 layout (`justhodl-dashboard-live`)

### Public-readable paths (per bucket policy)
- `data/*` — primary data files (report.json, fred-cache.json, etc.)
- `screener/*` — stock screener output
- `sentiment/*` — sentiment analysis output
- `flow-data.json` — options/fund flow data
- `crypto-intel.json` — crypto intelligence

### Private (boto3 SDK access only)
- `repo-data.json` — repo monitor stress scores
- `edge-data.json` — edge engine composite scores
- `intelligence-report.json` — justhodl-intelligence aggregated output
- `predictions.json` — STALE (30+ hours, ml-predictions writer broken since CF migration)
- `valuations-data.json` — monthly CAPE/Buffett indicator output
- `calibration/*` — calibrator history
- `learning/*` — signal-logger metadata
- `archive/*` — historical snapshots (1,665 files, 29MB)

### Critical files (most-frequent updates)
| Key | Writer | Frequency | Notes |
|---|---|---|---|
| `data/report.json` | daily-report-v3 | every 5 min | The current source of truth (188 stocks + FRED + regime) |
| `crypto-intel.json` | crypto-intel | every 6h | BTC/ETH/SOL technicals + on-chain |
| `edge-data.json` | edge-engine | every 6h | Composite ML risk score, regime |
| `intelligence-report.json` | justhodl-intelligence | hourly weekdays | Cross-system synthesis (FIXED 2026-04-25) |
| `repo-data.json` | repo-monitor | every 30 min weekdays | Plumbing stress score |
| `flow-data.json` | options-flow | every 4h | Options flow, fund flows |
| `screener/data.json` | stock-screener | every 4h | 503 stocks, Piotroski/Altman scores |

## Cloudflare

**Account:** `2e120c8358c6c85dcaba07eb16947817`

**Worker:** `justhodl-ai-proxy`
- Routes: `api.justhodl.ai` (custom domain) + `justhodl-ai-proxy.raafouis.workers.dev`
- Forwards POST → AWS Lambda `justhodl-ai-chat`
- Origin allowlist: `https://justhodl.ai`, `https://www.justhodl.ai`
- Adds auth token from secret `AI_CHAT_TOKEN`
- Body size cap: 32KB
- Source: `cloudflare/workers/justhodl-ai-proxy/src/index.js`
- Deploys via GitHub Actions on push (`.github/workflows/deploy-workers.yml`)

## CI/CD

**Repo:** `ElMooro/si` (justhodl.ai is GitHub Pages from this)

**Workflows:**
- `deploy-lambdas.yml` — deploys any `aws/lambdas/<name>/source/` change to AWS
- `deploy-workers.yml` — deploys `cloudflare/workers/*/` to CF on push
- `run-ops.yml` — runs any `aws/ops/pending/*.py` script with AWS creds; auto-commits reports/audit/lambdas back to repo
- `rotate-dex-scanner-pat.yml` — manual workflow_dispatch for GitHub PAT rotation

**IAM:** `github-actions-justhodl` — has 9 attached policies including AmazonDynamoDBReadOnlyAccess
**Secrets:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `ANTHROPIC_API_KEY`, `ANTHROPIC_API_KEY_NEW`, `TELEGRAM_BOT_TOKEN`

## Known broken / stale (as of 2026-04-25)

- **`ml-predictions`** Lambda — silently broken since the April 22 CF Worker migration. Calls `api.justhodl.ai` for bundled data; gets HTTP 403. Lambda catches the exception and returns success (CloudWatch shows 0 errors) but `predictions.json` doesn't update. **Decision: not retired**, the chokepoint downstream (`justhodl-intelligence`) was fixed instead via adapter pattern reading from `data/report.json` directly. ml-predictions itself stays broken but no longer affects the system.

- **`data.json`** at S3 root — 65 days stale orphan. Was the original aggregated data file before the daily-report-v3 architecture replaced it. Some old Lambdas (e.g. justhodl-intelligence pre-2026-04-25 fix) still tried to read it. Safe to delete after confirming no remaining consumers.

- **22 empty DynamoDB tables** — leftover from architecture experiments. Safe to delete but low priority.

- **Binance API geoblock** — `justhodl-crypto-intel` modules `fetch_oi` + `fetch_technicals` get HTTP 451 from Binance because AWS us-east-1 IPs are blocked. 15/17 modules still working; migration to Bybit/OKX/CoinGecko deferred.

## Open roadmap (dependency-respecting)

1. **Sunday April 26 9 AM UTC** — first real calibration run (post Week 1 fixes)
2. **Week 2A** — DONE 2026-04-25 (predictions schema v2, 7 enriched call sites)
3. **Week 2B** — Backtester Lambda, NEEDS 2-3 weeks of real outcomes
4. **Week 3A** — Daily Ranker, NEEDS calibrator weights with n≥10 per signal
5. **Week 3B** — Position sizing layer, NEEDS ranker
