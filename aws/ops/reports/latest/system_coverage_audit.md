# System Coverage Audit — every feature on website?

**Status:** failure  
**Duration:** 20.6s  
**Finished:** 2026-04-26T00:49:17+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/190_system_coverage_audit.py", line 74, in <module>
    rules_resp = events.list_rules(Limit=200)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the ListRules operation: 1 validation error detected: Value '200' at 'limit' failed to satisfy constraint: Member must have value less than or equal to 100

```

## Log
## A. Lambda inventory (full account)

- `00:48:57`   Total: 106 Lambdas
- `00:48:57`   justhodl-* : 48
- `00:48:57`   other      : 58
- `00:48:57` 
  Looking for Lambdas with Function URLs (browser-exposed):
- `00:49:16`   Found 66 Lambdas with public Function URLs:
- `00:49:16`     FinancialIntelligence-Backend            NONE     mod=2026-04-25
- `00:49:16`     MLPredictor                              NONE     mod=2025-05-30
- `00:49:16`     OpenBBS3DataProxy                        NONE     mod=2026-04-25
- `00:49:16`     aiapi-market-analyzer                    NONE     mod=2026-04-25
- `00:49:16`     alphavantage-market-agent                NONE     mod=2026-04-25
- `00:49:16`     alphavantage-technical-analysis          NONE     mod=2026-04-25
- `00:49:16`     autonomous-ai-processor                  NONE     mod=2026-04-25
- `00:49:16`     bea-economic-agent                       NONE     mod=2026-04-25
- `00:49:16`     benzinga-news-agent                      NONE     mod=2026-04-25
- `00:49:16`     bls-labor-agent                          NONE     mod=2026-04-25
- `00:49:16`     bond-indices-agent                       NONE     mod=2026-04-25
- `00:49:16`     census-economic-agent                    NONE     mod=2026-04-25
- `00:49:16`     cftc-futures-positioning-agent           NONE     mod=2026-04-25
- `00:49:16`     chatgpt-agent-api                        NONE     mod=2026-04-25
- `00:49:16`     coinmarketcap-agent                      NONE     mod=2026-04-25
- `00:49:16`     createEnhancedIndex                      NONE     mod=2026-04-25
- `00:49:16`     createUniversalIndex                     NONE     mod=2026-04-25
- `00:49:16`     dollar-strength-agent                    NONE     mod=2026-04-25
- `00:49:16`     economyapi                               NONE     mod=2026-04-25
- `00:49:16`     eia-energy-agent                         NONE     mod=2026-04-25
- `00:49:16`     enhanced-repo-agent                      NONE     mod=2026-04-25
- `00:49:16`     fedliquidityapi                          NONE     mod=2026-04-25
- `00:49:16`     fedliquidityapi-test                     NONE     mod=2026-04-25
- `00:49:16`     fmp-fundamentals-agent                   NONE     mod=2026-04-25
- `00:49:16`     fred-ice-bofa-api                        NONE     mod=2026-04-25
- `00:49:16`     global-liquidity-agent-v2                NONE     mod=2026-04-25
- `00:49:16`     google-trends-agent                      NONE     mod=2026-04-25
- `00:49:16`     justhodl-advanced-charts                 NONE     mod=2026-04-25
- `00:49:16`     justhodl-ai-chat                         NONE     mod=2026-04-25
- `00:49:16`     justhodl-cache-layer                     NONE     mod=2026-04-25
- `00:49:16`     justhodl-charts-agent                    NONE     mod=2026-04-25
- `00:49:16`     justhodl-crypto-enricher                 NONE     mod=2026-04-25
- `00:49:16`     justhodl-dex-scanner                     NONE     mod=2026-04-25
- `00:49:16`     justhodl-ecb-proxy                       NONE     mod=2026-04-25
- `00:49:16`     justhodl-edge-engine                     NONE     mod=2026-04-25
- `00:49:16`     justhodl-financial-secretary             NONE     mod=2026-04-25
- `00:49:16`     justhodl-fred-proxy                      NONE     mod=2026-04-25
- `00:49:16`     justhodl-investor-agents                 NONE     mod=2026-04-25
- `00:49:16`     justhodl-khalid-metrics                  NONE     mod=2026-04-25
- `00:49:16`     justhodl-news-sentiment                  NONE     mod=2026-04-25
- `00:49:16`     justhodl-options-flow                    NONE     mod=2026-04-25
- `00:49:16`     justhodl-stock-ai-research               NONE     mod=2026-04-25
- `00:49:16`     justhodl-stock-analyzer                  NONE     mod=2026-04-25
- `00:49:16`     justhodl-stock-screener                  NONE     mod=2026-04-26
- `00:49:16`     justhodl-telegram-bot                    NONE     mod=2026-04-25
- `00:49:16`     justhodl-treasury-proxy                  NONE     mod=2026-04-25
- `00:49:16`     justhodl-ultimate-orchestrator           NONE     mod=2026-04-25
- `00:49:16`     justhodl-ultimate-trading                NONE     mod=2026-04-25
- `00:49:16`     justhodl-valuations-agent                NONE     mod=2026-04-25
- `00:49:16`     macro-financial-intelligence             NONE     mod=2026-04-25
- `00:49:16`     macro-financial-report-viewer            NONE     mod=2026-04-25
- `00:49:16`     macro-report-api                         NONE     mod=2026-04-25
- `00:49:16`     manufacturing-global-agent               NONE     mod=2026-04-25
- `00:49:16`     multi-agent-orchestrator                 NONE     mod=2025-09-21
- `00:49:16`     nasdaq-datalink-agent                    NONE     mod=2026-04-25
- `00:49:16`     news-sentiment-agent                     NONE     mod=2026-04-25
- `00:49:16`     openbb-websocket-broadcast               NONE     mod=2025-05-30
- `00:49:16`     openbb-websocket-handler                 NONE     mod=2025-06-15
- `00:49:16`     scrapeMacroData                          NONE     mod=2025-06-03
- `00:49:16`     securities-banking-agent                 NONE     mod=2026-04-25
- `00:49:16`     testEnhancedScraper                      NONE     mod=2026-04-25
- `00:49:16`     treasury-api                             NONE     mod=2026-04-25
- `00:49:16`     ultimate-multi-agent                     NONE     mod=2025-09-21
- `00:49:16`     universal-agent-gateway                  NONE     mod=2026-04-25
- `00:49:16`     volatility-monitor-agent                 NONE     mod=2026-04-25
- `00:49:16`     xccy-basis-agent                         NONE     mod=2026-04-25
## B. EventBridge schedules (auto-running Lambdas)

