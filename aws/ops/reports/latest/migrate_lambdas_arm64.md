# Migrate Python Lambdas to arm64 (Graviton2) — 20% cheaper

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-04-25T10:21:53+00:00  

## Data

| coverage_pct | eligible | estimated_monthly_savings | failed | migrated | reverted |
|---|---|---|---|---|---|
| 0% | 81 | $0.00 | 81 | 0 | 0 |

## Log
## 1. Inventory eligible Lambdas

- `10:21:53`   Total Lambdas: 97
- `10:21:53`   Eligible for arm64 migration: 81
- `10:21:53`   Skipped: 16 (16 for safety)
- `10:21:53`     legacy_prefix: 3 (e.g. openbb-websocket-broadcast, openbb-websocket-handler, openbb-system2-api)
- `10:21:53`     snapstart_recent: 8 (e.g. justhodl-stock-analyzer, justhodl-ai-chat, justhodl-stock-screener)
- `10:21:53`     heavy_deps: 4 (e.g. ultimate-multi-agent, scrapeMacroData, MLPredictor)
- `10:21:53`     runtime=nodejs18.x: 1 (e.g. bls-employment-api-v2)
## 2. Migrate 81 Lambdas

- `10:21:53` ✗   [ 1/81] justhodl-data-collector: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [ 2/81] ofrapi: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [ 3/81] bls-labor-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [ 4/81] justhodl-ultimate-orchestrator: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [ 5/81] justhodl-crypto-enricher: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [ 6/81] volatility-monitor-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [ 7/81] fedliquidityapi-test: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [ 8/81] macro-report-api: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [ 9/81] bond-indices-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [10/81] ecb-data-daily-updater: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [11/81] google-trends-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [12/81] OpenBBS3DataProxy: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [13/81] justhodl-calibrator: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [14/81] justhodl-email-reports-v2: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [15/81] fmp-fundamentals-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [16/81] eia-energy-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [17/81] justhodl-liquidity-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [18/81] ecb-auto-updater: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [19/81] FinancialIntelligence-Backend: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [20/81] justhodl-daily-macro-report: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [21/81] macro-financial-report-viewer: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [22/81] justhodl-crypto-intel: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [23/81] createEnhancedIndex: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [24/81] census-economic-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [25/81] dollar-strength-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [26/81] economyapi: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [27/81] aiapi-market-analyzer: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [28/81] autonomous-ai-processor: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [29/81] treasury-auto-updater: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [30/81] permanent-market-intelligence: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [31/81] justhodl-dex-scanner: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [32/81] ecb: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [33/81] justhodl-repo-monitor: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [34/81] global-liquidity-agent-v2: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [35/81] justhodl-treasury-proxy: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [36/81] justhodl-daily-report-v3: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [37/81] daily-liquidity-report: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [38/81] fred-ice-bofa-api: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [39/81] justhodl-intelligence: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [40/81] justhodl-ultimate-trading: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [41/81] xccy-basis-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [42/81] coinmarketcap-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [43/81] alphavantage-market-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [44/81] justhodl-options-flow: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [45/81] justhodl-email-reports: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [46/81] treasury-api: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [47/81] benzinga-news-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [48/81] justhodl-outcome-checker: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [49/81] justhodl-khalid-metrics: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [50/81] securities-banking-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [51/81] testEnhancedScraper: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [52/81] createUniversalIndex: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [53/81] justhodl-ml-predictions: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [54/81] justhodl-telegram-bot: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [55/81] nyfed-primary-dealer-fetcher: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [56/81] justhodl-bloomberg-v8: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [57/81] justhodl-news-sentiment: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [58/81] fedliquidityapi: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [59/81] justhodl-fred-proxy: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [60/81] enhanced-repo-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [61/81] manufacturing-global-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [62/81] justhodl-signal-logger: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [63/81] global-liquidity-agent-TEST: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [64/81] justhodl-ecb-proxy: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [65/81] bea-economic-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [66/81] fmp-stock-picks-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [67/81] justhodl-charts-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [68/81] justhodl-health-monitor: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [69/81] justhodl-chat-api: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [70/81] nyfed-financial-stability-fetcher: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [71/81] justhodl-cache-layer: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [72/81] alphavantage-technical-analysis: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [73/81] news-sentiment-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [74/81] nyfedapi-isolated: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [75/81] macro-financial-intelligence: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [76/81] justhodl-financial-secretary: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [77/81] justhodl-advanced-charts: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [78/81] nasdaq-datalink-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [79/81] chatgpt-agent-api: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [80/81] universal-agent-gateway: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
- `10:21:53` ✗   [81/81] justhodl-valuations-agent: Parameter validation failed:
Unknown parameter in input: "Architectures", must be one of: FunctionName, Role, Handler, Description, Timeout, MemorySize, VpcConfig, Environment, Runtime, DeadLetterConfig, KMSKeyArn, TracingConfig, RevisionId, Layers, FileSystemConfigs, ImageConfig, EphemeralStorage, SnapStart, LoggingConfig, CapacityProviderConfig, DurableConfig
## 3. Summary

- `10:21:53`   ✅ Migrated: 0
- `10:21:53`   🔄 Reverted (arm64 incompat): 0
- `10:21:53`   ❌ Failed: 81
- `10:21:53` 
  Migration coverage: 0% of fleet on arm64
- `10:21:53`   Estimated savings: ~$0.00/mo
- `10:21:53` Done
