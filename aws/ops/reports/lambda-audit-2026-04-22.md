# Lambda Sprawl Audit — Dry Run

**Region:** `us-east-1`  
**Total functions:** 123  
**Lookback window:** 90 days  
**Generated:** 2026-04-22T17:24:35+00:00

## Summary

| Status | Count | Total CloudWatch logs |
|--------|-------|------------------------|
| 🟢 Active (invoked in last 90d) | **27** | 92.3 MB |
| 🟡 Review (no invocations but has URL or EB rule) | **60** | 605.1 MB |
| 🔴 Kill candidate (safe to delete) | **36** | 134.5 MB |
| **Total** | **123** | **831.9 MB** |

**Estimated cleanup:** deleting the 36 kill candidates frees 134.5 MB of CloudWatch logs (~$0.03/GB/month storage).

## 🔴 Kill candidates

Zero invocations in 90 days, no Function URL, no EventBridge rule. Safe to delete after spot-check.

| Function | Runtime | Last modified | Code | Logs |
|----------|---------|---------------|------|------|
| `ai-prediction-agent` | python3.9 | 2025-09-21 | 0 KB | — |
| `autonomous-ai-system` | python3.11 | 2025-09-30 | 4294 KB | 6.5 KB |
| `bls-employment-data-api` | nodejs18.x | 2025-08-18 | 3 KB | 3.0 KB |
| `bls-function-bls-minimal` | nodejs18.x | 2025-08-17 | 2 KB | 7.9 KB |
| `chatgpt-agent` | python3.9 | 2025-09-21 | 0 KB | — |
| `crisisapi` | python3.9 | 2025-08-15 | 9 KB | 356.2 KB |
| `ecb` | python3.9 | 2025-08-12 | 2 KB | 4.0 MB |
| `ecb-auto-updater` | python3.9 | 2025-08-12 | 3 KB | 71.4 KB |
| `ecb-data-daily-updater` | python3.9 | 2025-08-10 | 2 KB | 454.2 KB |
| `ecb-data-service` | python3.9 | 2025-08-11 | 7 KB | 277.0 KB |
| `enhanced-openbb-handler` | python3.9 | 2025-08-10 | 2 KB | 101.6 MB |
| `fed-data-v2` | python3.12 | 2025-09-07 | 5 KB | 18.9 KB |
| `fed-liquidity-agent` | python3.9 | 2025-09-21 | 0 KB | — |
| `financial-dashboard-api-function` | nodejs20.x | 2025-05-07 | 4 KB | 21.9 KB |
| `getSupabaseConfig` | python3.11 | 2025-05-30 | 1 KB | 92.4 KB |
| `global-liquidity-agent` | python3.9 | 2025-09-21 | 0 KB | — |
| `indexToOpenSearch` | python3.11 | 2025-06-13 | 8 KB | 11.6 MB |
| `justhodl-calibrator` | python3.12 | 2026-03-11 | 4 KB | 7.1 KB |
| `justhodl-daily-report` | python3.11 | 2025-09-24 | 43634 KB | 1.8 KB |
| `justhodl-data-collector` | python3.9 | 2025-09-21 | 1 KB | 8.2 MB |
| `justhodl-email-reports-v2` | python3.11 | 2025-10-05 | 6 KB | 161.7 KB |
| `justhodl-liquidity-agent` | python3.12 | 2026-03-12 | 7 KB | 76.5 KB |
| `nyfed-cmdi-fetcher` | python3.9 | 2025-08-17 | 3 KB | 31.7 KB |
| `nyfed-main-aggregator` | python3.9 | 2025-08-17 | 2 KB | 16.8 KB |
| `ofrapi` | python3.9 | 2025-08-13 | 21 KB | 525.0 KB |
| `openbb-combined-daily-reports` | python3.9 | 2025-08-10 | 4 KB | 1.2 KB |
| `openbb-correlation-analysis` | python3.9 | 2025-08-10 | 1 KB | 1.5 KB |
| `openbb-daily-risk-report` | python3.9 | 2025-08-10 | 2 KB | 1007.0 B |
| `openbb-graphql-handler` | python3.9 | 2025-06-15 | 762 KB | 3.9 KB |
| `openbb-ml-predictions` | python3.9 | 2025-08-10 | 1 KB | 1.2 KB |
| `openbb-system2-proxy` | nodejs18.x | 2025-06-30 | 1 KB | — |
| `openbb-trading-signals` | python3.9 | 2025-08-10 | 1 KB | 1.7 KB |
| `openbb-vix-alert` | python3.9 | 2025-08-10 | 1 KB | 911.0 B |
| `polygon-api` | python3.9 | 2025-09-10 | 6 KB | 7.0 MB |
| `testNewDataSources` | python3.11 | 2025-05-30 | 1 KB | — |
| `unified-openbb-handler` | python3.11 | 2025-06-15 | 6 KB | 25.3 KB |

## 🟡 Review candidates

No invocations but has a Function URL or EventBridge rule. Could be:
- Scheduled job that's silently failing (check CloudWatch logs)
- Publicly reachable URL that nobody is calling (consider disabling URL then deleting)
- Rarely-invoked manual fixture

| Function | Runtime | URL | EventBridge Rules | Scheduler | Last modified | Logs |
|----------|---------|-----|-------------------|-----------|---------------|------|
| `FinancialIntelligence-Backend` | python3.11 | ✓ | — | — | 2025-10-08 | 128.3 KB |
| `MLPredictor` | python3.10 | ✓ | `MLPredictorDaily` | — | 2025-05-30 | 315.5 KB |
| `OpenBBS3DataProxy` | python3.11 | ✓ | — | — | 2025-05-30 | 63.4 KB |
| `aiapi-market-analyzer` | python3.13 | ✓ | `aiapi-daily-analysis`, `aiapi-hourly-predictions`, `aiapi-market-data-collection` (+1) | — | 2025-09-14 | 17.4 MB |
| `alphavantage-market-agent` | python3.9 | ✓ | `alphavantage-market-update` | — | 2025-09-21 | 3.1 MB |
| `autonomous-ai-processor` | python3.11 | ✓ | `autonomous-ai-schedule` | — | 2025-09-30 | 16.3 KB |
| `benzinga-news-agent` | python3.12 | ✓ | — | — | 2026-02-28 | 3.6 KB |
| `bls-labor-agent` | python3.9 | ✓ | — | — | 2025-09-19 | 1.6 MB |
| `bond-indices-agent` | python3.9 | ✓ | — | — | 2025-09-19 | 22.2 MB |
| `census-economic-agent` | python3.9 | ✓ | — | — | 2025-09-20 | 5.0 MB |
| `cftc-futures-positioning-agent` | python3.11 | ✓ | `cftc-cot-weekly-update` | — | 2026-03-03 | 28.7 MB |
| `coinmarketcap-agent` | python3.9 | ✓ | — | — | 2025-09-19 | 8.7 MB |
| `createEnhancedIndex` | python3.11 | ✓ | — | — | 2025-05-30 | 2.1 KB |
| `createUniversalIndex` | python3.11 | ✓ | — | — | 2025-05-30 | 74.1 KB |
| `daily-liquidity-report` | python3.11 |  | `daily-liquidity-7am` | — | 2025-10-05 | 433.2 KB |
| `dollar-strength-agent` | python3.9 | ✓ | — | — | 2025-09-19 | 4.4 MB |
| `economyapi` | python3.9 | ✓ | — | — | 2025-09-09 | 1004.2 KB |
| `eia-energy-agent` | python3.12 | ✓ | — | — | 2026-02-28 | 32.5 KB |
| `fedliquidityapi-test` | python3.12 | ✓ | — | — | 2025-09-06 | 2.8 KB |
| `fmp-fundamentals-agent` | python3.12 | ✓ | — | — | 2026-02-28 | 4.2 KB |
| `fred-ice-bofa-api` | python3.9 | ✓ | `ice-bofa-3x-weekly` | — | 2025-09-20 | 5.6 MB |
| `global-liquidity-agent-TEST` | python3.11 |  | `liquidity-realtime-TEST` | — | 2025-09-14 | 448.9 KB |
| `global-liquidity-agent-v2` | python3.11 | ✓ | `DailyLiquidityReportRule`, `khalid-daily-report`, `liquidity-critical-monitor` (+5) | — | 2025-11-01 | 16.3 MB |
| `google-trends-agent` | python3.11 | ✓ | — | — | 2025-09-25 | 1.6 MB |
| `justhodl-advanced-charts` | python3.9 | ✓ | — | — | 2025-09-22 | 14.5 KB |
| `justhodl-ai-chat` | python3.12 | ✓ | — | — | 2026-04-22 | 452.6 KB |
| `justhodl-cache-layer` | python3.9 | ✓ | — | — | 2025-09-21 | 547.0 B |
| `justhodl-charts-agent` | python3.9 | ✓ | — | — | 2025-09-22 | 59.4 KB |
| `justhodl-crypto-enricher` | python3.12 | ✓ | — | — | 2026-03-03 | 91.2 KB |
| `justhodl-crypto-intel` | python3.12 |  | `justhodl-crypto-15min`, `justhodl-crypto-intel-schedule` | — | 2026-03-05 | 42.8 MB |
| `justhodl-daily-macro-report` | python3.11 |  | `DailyMacroReportRule` | — | 2025-09-24 | — |
| `justhodl-daily-report-v3` | python3.12 |  | `justhodl-daily-8am`, `justhodl-daily-v3`, `justhodl-v9-auto-refresh` (+2) | — | 2026-03-05 | 48.7 MB |
| `justhodl-dex-scanner` | python3.12 | ✓ | `justhodl-dex-scanner-15min` | — | 2026-03-08 | 1.9 MB |
| `justhodl-email-reports` | python3.12 |  | `justhodl-8am` | — | 2025-09-24 | 3.5 KB |
| `justhodl-intelligence` | python3.12 |  | `justhodl-intel-daily`, `justhodl-intel-hourly` | — | 2026-02-23 | 608.1 KB |
| `justhodl-investor-agents` | python3.11 | ✓ | — | — | 2026-04-22 | 14.1 KB |
| `justhodl-khalid-metrics` | python3.12 | ✓ | `justhodl-khalid-metrics-refresh` | — | 2026-02-27 | 683.7 KB |
| `justhodl-options-flow` | python3.11 | ✓ | `justhodl-flow-refresh` | — | 2026-03-05 | 6.5 MB |
| `justhodl-outcome-checker` | python3.12 |  | `justhodl-outcome-checker-weekly` | — | 2026-03-11 | 221.8 KB |
| `justhodl-repo-monitor` | python3.12 |  | `justhodl-repo-30min`, `justhodl-repo-daily` | — | 2026-02-23 | 3.9 MB |
| `justhodl-stock-analyzer` | python3.12 | ✓ | — | — | 2026-03-09 | 23.5 KB |
| `justhodl-stock-screener` | python3.11 | ✓ | — | — | 2026-03-11 | 2.5 MB |
| `justhodl-treasury-proxy` | python3.12 | ✓ | — | — | 2026-02-22 | 13.6 KB |
| `justhodl-ultimate-orchestrator` | python3.9 | ✓ | — | — | 2026-02-20 | 35.1 MB |
| `justhodl-ultimate-trading` | python3.9 | ✓ | — | — | 2025-09-22 | 11.3 KB |
| `macro-financial-report-viewer` | python3.11 | ✓ | — | — | 2025-10-03 | 904.0 B |
| `macro-report-api` | python3.9 | ✓ | — | — | 2025-09-19 | 82.7 KB |
| `multi-agent-orchestrator` | python3.11 | ✓ | — | — | 2025-09-21 | 7.4 KB |
| `openbb-websocket-broadcast` | python3.11 | ✓ | — | — | 2025-05-30 | 2.2 KB |
| `openbb-websocket-handler` | python3.11 | ✓ | — | — | 2025-06-15 | 1.1 MB |
| `permanent-market-intelligence` | python3.9 |  | `permanent-market-schedule` | — | 2025-09-24 | 153.5 KB |
| `scrapeMacroData` | python3.11 | ✓ | `DailyMacroScraper` | — | 2025-06-03 | 307.7 MB |
| `securities-banking-agent` | python3.9 | ✓ | — | — | 2025-09-19 | 4.6 MB |
| `testEnhancedScraper` | python3.11 | ✓ | — | — | 2025-05-30 | 2.3 KB |
| `treasury-api` | python3.9 | ✓ | `treasury-auto-update` | — | 2025-09-21 | 13.1 MB |
| `treasury-auto-updater` | python3.9 |  | `treasury-monday-update`, `treasury-thursday-update` | — | 2025-08-13 | 295.5 KB |
| `ultimate-multi-agent` | python3.11 | ✓ | — | — | 2025-09-21 | 47.7 KB |
| `universal-agent-gateway` | python3.9 | ✓ | — | — | 2025-09-21 | 239.0 KB |
| `volatility-monitor-agent` | python3.9 | ✓ | — | — | 2025-09-19 | 3.2 MB |
| `xccy-basis-agent` | python3.9 | ✓ | `xccy-basis-30min` | — | 2025-09-19 | 14.2 MB |

## 🟢 Active functions

Invoked within the last 90 days.

| Function | Runtime | 90d invocations | Last | URL | EB rules | Scheduler |
|----------|---------|-----------------|------|-----|----------|-----------|
| `justhodl-bloomberg-v8` | python3.12 | 17,858 | 2026-04-22 |  | 1 |  |
| `openbb-system2-api` | python3.9 | 7,125 | 2026-04-22 |  |  |  |
| `macro-financial-intelligence` | python3.11 | 4,047 | 2026-04-22 | ✓ | 1 |  |
| `enhanced-repo-agent` | python3.9 | 3,985 | 2026-04-22 | ✓ | 1 |  |
| `fedliquidityapi` | python3.9 | 3,985 | 2026-04-22 | ✓ | 1 |  |
| `bea-economic-agent` | python3.9 | 3,957 | 2026-04-22 | ✓ |  |  |
| `chatgpt-agent-api` | python3.11 | 3,957 | 2026-04-22 | ✓ | 1 |  |
| `news-sentiment-agent` | python3.9 | 3,957 | 2026-04-22 | ✓ | 1 |  |
| `manufacturing-global-agent` | python3.9 | 3,956 | 2026-04-22 | ✓ |  |  |
| `justhodl-ml-predictions` | python3.12 | 746 | 2026-04-21 |  | 2 |  |
| `fmp-stock-picks-agent` | python3.12 | 685 | 2026-04-21 |  | 2 |  |
| `justhodl-telegram-bot` | python3.12 | 650 | 2026-04-22 | ✓ | 1 |  |
| `justhodl-financial-secretary` | python3.12 | 454 | 2026-04-21 | ✓ | 1 |  |
| `justhodl-fred-proxy` | python3.12 | 304 | 2026-04-07 | ✓ |  |  |
| `justhodl-edge-engine` | python3.12 | 200 | 2026-04-21 | ✓ | 1 |  |
| `justhodl-signal-logger` | python3.12 | 176 | 2026-04-21 |  | 1 |  |
| `justhodl-morning-intelligence` | python3.12 | 93 | 2026-04-21 |  | 1 |  |
| `justhodl-ecb-proxy` | python3.12 | 46 | 2026-02-22 | ✓ |  |  |
| `justhodl-news-sentiment` | python3.11 | 44 | 2026-04-21 | ✓ | 1 |  |
| `alphavantage-technical-analysis` | python3.9 | 28 | 2026-02-18 | ✓ |  |  |
| `bls-employment-api-v2` | nodejs18.x | 28 | 2026-04-21 |  | 2 |  |
| `justhodl-valuations-agent` | python3.11 | 15 | 2026-03-31 | ✓ | 1 |  |
| `nasdaq-datalink-agent` | python3.12 | 15 | 2026-04-06 | ✓ |  |  |
| `justhodl-chat-api` | python3.12 | 4 | 2026-02-17 |  |  |  |
| `nyfedapi-isolated` | python3.9 | 3 | 2026-04-05 |  |  |  |
| `nyfed-financial-stability-fetcher` | python3.9 | 1 | 2026-04-05 |  |  |  |
| `nyfed-primary-dealer-fetcher` | python3.9 | 1 | 2026-04-05 |  |  |  |

## Next step

Review the kill candidates. When ready, Claude will push a companion script `delete_lambda_sprawl.py` that deletes the approved list — also idempotent, also dry-runnable with `--dry-run`.