# Plan vs usage — find paid features we're not using

**Status:** success  
**Duration:** 17.8s  
**Finished:** 2026-04-25T10:13:34+00:00  

## Data

| black | findings | red | yellow |
|---|---|---|---|
| 0 | 8 | 1 | 7 |

## Log
## A1. Lambda: runtime versions, memory tiers, concurrency

- `10:13:17`   Total Lambda functions: 97
- `10:13:17`   Runtime distribution:
- `10:13:17`     python3.9                 36
- `10:13:17`     python3.12                32
- `10:13:17`     python3.11                26
- `10:13:17`     python3.13                1
- `10:13:17`     python3.10                1
- `10:13:17`     nodejs18.x                1
- `10:13:17`   Memory tier distribution:
- `10:13:17`       128MB  3
- `10:13:17`       256MB  24
- `10:13:17`       512MB  42
- `10:13:17`       768MB  1
- `10:13:17`      1024MB  22
- `10:13:17`      2048MB  2
- `10:13:17`      3008MB  2
- `10:13:17`     10240MB  1
- `10:13:22`   Lambdas with reserved concurrency: 4
- `10:13:22`     justhodl-ai-chat                           RC=3
- `10:13:22`     ultimate-multi-agent                       RC=100
- `10:13:22`     global-liquidity-agent-v2                  RC=100
- `10:13:22`     justhodl-daily-report-v3                   RC=1
- `10:13:26`   Provisioned concurrency configs: 0
- `10:13:26`   Lambdas using Layers: 3
- `10:13:26`     scrapeMacroData: 1 layer(s)
- `10:13:26`     MLPredictor: 1 layer(s)
- `10:13:26`     multi-agent-orchestrator: 1 layer(s)
- `10:13:31`   SnapStart eligible Lambdas (Python 3.12/3.13/Java): 33
- `10:13:31`     Currently using SnapStart: 0 (sample of 10)
- `10:13:31`   Architecture distribution:
- `10:13:31`     x86_64: 97
## A2. S3 features

- `10:13:31`   Intelligent Tiering configs: 0
- `10:13:31`   Transfer Acceleration: Disabled
- `10:13:31`   S3 Inventory configs: 0
- `10:13:31`   Versioning: Disabled
- `10:13:31`   Lifecycle rules: 1
- `10:13:31`     archive-to-glacier-deep-after-90d: Enabled
- `10:13:31`   StandardStorage                   26.41 GB
## A3. DynamoDB features

- `10:13:32`   Total tables: 7
- `10:13:33`   Billing modes: {'PAY_PER_REQUEST': 7}
- `10:13:33`   Tables with Streams enabled: 0
- `10:13:33`   Tables with PITR enabled: 0
- `10:13:33`   Total GSIs across all tables: 0
## A4. CloudWatch features

- `10:13:33`   Alarms configured: 26
- `10:13:33`   Dashboards: 3
- `10:13:33`     JustHodl
- `10:13:33`     MacroPlatformStatus
- `10:13:33`     OpenBB-Platform-v17-1-Operational
## A5. SSM Parameter Store

- `10:13:33`   Total parameters: 6 (Standard: 6, Advanced: 0)
## A6. EventBridge custom event buses

- `10:13:33`   Event buses: 1
- `10:13:33`     default (default=True)
## A7. IAM users — unused?

- `10:13:34`   IAM users: 3
- `10:13:34`     fred-api-access                     keys=0 pwd_last_used=None
- `10:13:34`     github-actions-justhodl             keys=1 pwd_last_used=None
- `10:13:34`       key AKIA4PMRPTXHDJ7JX75Q... last used: 2026-04-25 10:05:00+00:00
- `10:13:34`     OPENBB                              keys=2 pwd_last_used=None
- `10:13:34`       key AKIA4PMRPTXHEXISCQWI... last used: 2025-08-18 00:48:00+00:00
- `10:13:34`       key AKIA4PMRPTXHLBZVGCWU... last used: 2026-02-20 19:42:00+00:00
## B. Cloudflare features

- `10:13:34`   CloudFlare resources currently provisioned (per separate tool query):
- `10:13:34`     Workers: 1 (justhodl-ai-proxy)
- `10:13:34`     D1 databases: 0
- `10:13:34`     KV namespaces: 0
- `10:13:34`     R2 buckets: 0
- `10:13:34`     Durable Objects: 0
- `10:13:34`     Queues: 0
## C. Third-party API plans — usage check

- `10:13:34`   FMP Premium endpoints in use:
- `10:13:34`     ✓ /stable/quote                       → real-time quote (premium)
- `10:13:34`   FMP Premium endpoints NOT in use:
- `10:13:34`     ✗ /api/v3/earning_call_transcript     → transcripts (premium)
- `10:13:34`     ✗ /api/v3/economic                    → economic calendar (premium)
- `10:13:34`     ✗ /api/v3/financial-statements        → fundamentals (premium)
- `10:13:34`     ✗ /api/v3/grade                       → analyst ratings (premium)
- `10:13:34`     ✗ /api/v3/historical-price-full       → premium historicals
- `10:13:34`     ✗ /api/v3/insider-trading             → insider trades (premium)
- `10:13:34`     ✗ /api/v3/sec_filings                 → SEC filings (premium)
- `10:13:34`     ✗ /api/v3/stock-screener              → advanced screener (premium)
- `10:13:34` 
  Polygon endpoints in use: ['/v2/aggs/ticker/', '/v2/last/trade', '/v3/reference/options']
- `10:13:34` 
  Anthropic Batch API in use: False
## Build audit doc

- `10:13:34` ✅   Wrote aws/ops/audit/plan_vs_usage_2026-04-25.md
- `10:13:34` Done
