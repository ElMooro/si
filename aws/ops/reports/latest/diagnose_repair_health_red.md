# Health-monitor RED triage — diagnose + auto-repair

**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-04-27T17:15:38+00:00  

## Log
- `17:15:34` Run timestamp: 2026-04-27T17:15:34+00:00
- `17:15:34` Region: us-east-1
## ── justhodl-data-collector ──

- `17:15:34`   Runtime:    python3.9
- `17:15:34`   Handler:    AWS='save_to_s3.lambda_handler', repo='save_to_s3.lambda_handler'
- `17:15:34`   LastMod:    2026-04-25T10:24:18
- `17:15:34`   CloudWatch (24h): Invocations=0, Errors=0, ErrRate=0.0%
- `17:15:35`   EB rule: ✗ justhodl-hourly-collection state=DISABLED schedule=rate(1 hour)
- `17:15:35` ✅   REPAIR: enabled rule 'justhodl-hourly-collection'
## ── justhodl-email-reports-v2 ──

- `17:15:35`   Runtime:    python3.11
- `17:15:35`   Handler:    AWS='lambda_function.lambda_handler', repo='lambda_function.lambda_handler'
- `17:15:35`   LastMod:    2026-04-26T12:18:10
- `17:15:35`   CloudWatch (24h): Invocations=0, Errors=0, ErrRate=0.0%
- `17:15:35`   EB rule: ✓ DailyEmailReportsV2 state=ENABLED schedule=cron(0 12 * * ? *)
## ── justhodl-khalid-metrics ──

- `17:15:35`   Runtime:    python3.12
- `17:15:35`   Handler:    AWS='lambda_function.lambda_handler', repo='lambda_function.lambda_handler'
- `17:15:35`   LastMod:    2026-04-25T10:27:46
- `17:15:35`   CloudWatch (24h): Invocations=0, Errors=0, ErrRate=0.0%
- `17:15:36` ⚠   EB rules: NONE pointing at this Lambda
## ── scrapeMacroData ──

- `17:15:36`   Runtime:    python3.11
- `17:15:36`   Handler:    AWS='lambda_function.lambda_handler', repo='lambda_function.lambda_handler'
- `17:15:36`   LastMod:    2025-06-03T18:10:33
- `17:15:36`   CloudWatch (24h): Invocations=0, Errors=0, ErrRate=0.0%
- `17:15:36`   EB rule: ✗ DailyMacroScraper state=DISABLED schedule=cron(0 12 * * ? *)
- `17:15:36` ✅   REPAIR: enabled rule 'DailyMacroScraper'
## ── fmp-stock-picks-agent ──

- `17:15:36`   Runtime:    python3.12
- `17:15:36`   Handler:    AWS='lambda_function.lambda_handler', repo='lambda_function.lambda_handler'
- `17:15:36`   LastMod:    2026-04-26T12:17:49
- `17:15:36`   CloudWatch (24h): Invocations=0, Errors=0, ErrRate=0.0%
- `17:15:36`   EB rule: ✗ fmp-movers-hourly state=DISABLED schedule=cron(0 14,16,18,20 ? * MON-FRI *)
- `17:15:36`   EB rule: ✗ fmp-stock-picks-daily state=DISABLED schedule=cron(0 12 ? * MON-FRI *)
- `17:15:36` ✅   REPAIR: enabled rule 'fmp-movers-hourly'
- `17:15:36` ✅   REPAIR: enabled rule 'fmp-stock-picks-daily'
## ── news-sentiment-agent ──

- `17:15:36`   Runtime:    python3.9
- `17:15:36`   Handler:    AWS='lambda_news_agent.lambda_handler', repo='lambda_news_agent.lambda_handler'
- `17:15:36`   LastMod:    2026-04-25T10:29:24
- `17:15:37`   CloudWatch (24h): Invocations=78, Errors=78, ErrRate=100.0%
- `17:15:37`   EB rule: ✗ news-sentiment-update state=DISABLED schedule=rate(30 minutes)
- `17:15:37` ✅   REPAIR: enabled rule 'news-sentiment-update'
## ── justhodl-intelligence ──

- `17:15:37`   Runtime:    python3.12
- `17:15:37`   Handler:    AWS='lambda_function.lambda_handler', repo='lambda_function.lambda_handler'
- `17:15:37`   LastMod:    2026-04-26T12:52:38
- `17:15:37`   CloudWatch (24h): Invocations=7, Errors=0, ErrRate=0.0%
- `17:15:37`   EB rule: ✓ justhodl-intel-daily state=ENABLED schedule=cron(10 12 * * ? *)
- `17:15:37`   EB rule: ✓ justhodl-intel-hourly state=ENABLED schedule=cron(5 12-23 ? * MON-FRI *)
## ── justhodl-repo-monitor ──

- `17:15:37`   Runtime:    python3.12
- `17:15:37`   Handler:    AWS='lambda_function.lambda_handler', repo='lambda_function.lambda_handler'
- `17:15:37`   LastMod:    2026-04-25T10:26:35
- `17:15:37`   CloudWatch (24h): Invocations=10, Errors=0, ErrRate=0.0%
- `17:15:38`   EB rule: ✓ justhodl-repo-30min state=ENABLED schedule=cron(0/30 13-23 ? * MON-FRI *)
- `17:15:38`   EB rule: ✓ justhodl-repo-daily state=ENABLED schedule=cron(0 12 * * ? *)
## Summary

- `17:15:38` 
  Lambdas inspected: 8
- `17:15:38`   Rules re-enabled:  5
- `17:15:38`     + justhodl-data-collector ← justhodl-hourly-collection
- `17:15:38`     + scrapeMacroData ← DailyMacroScraper
- `17:15:38`     + fmp-stock-picks-agent ← fmp-movers-hourly
- `17:15:38`     + fmp-stock-picks-agent ← fmp-stock-picks-daily
- `17:15:38`     + news-sentiment-agent ← news-sentiment-update
- `17:15:38`   Handlers fixed:    0
- `17:15:38` ⚠ 
  Lambdas with NO EB rules (need investigation):
- `17:15:38`     - justhodl-khalid-metrics: 0 invocations, 0 errors
- `17:15:38` 
  Machine-readable findings: aws/ops/reports/latest/health_red_triage.json
