# Follow-up — Secretary JSON, EB duplicates, FRED skip rate

**Status:** success  
**Duration:** 10.8s  
**Finished:** 2026-04-23T17:34:09+00:00  

## Data

| avg_done_s | avg_fetch_s | check | duplicate_targets | skip_rate_pct |
|---|---|---|---|---|
|  |  | eb-duplicates | 13 |  |
| 110.8 | 19.9 |  |  | 89.0 |

## Log
## A. Secretary v2.2 — actual JSON shape

- `17:33:58`   All top-level keys: ['ai_briefing', 'cftc', 'crypto_count', 'crypto_top10', 'data_freshness', 'deltas', 'fear_greed', 'fred', 'liquidity', 'market_snapshot', 'news', 'recommendations', 'risk', 'scan_time_seconds', 'stocks_count', 'tier2', 'timestamp', 'top_buys', 'type', 'version']
- `17:33:58` 
- `17:33:58`   options_flow: ✗ NOT FOUND under any of ['options_flow', 'options', 'flow', 'tier2_flow', 'options_card']
- `17:33:58`   crypto_intel: ✗ NOT FOUND under any of ['crypto_intel', 'crypto', 'crypto_card', 'tier2_crypto']
- `17:33:58`   sector_rotation: ✗ NOT FOUND under any of ['sector_rotation', 'sectors', 'rotation', 'sector_card']
- `17:33:58`   hit_rate: key='deltas', value_preview={'available': True, 'yesterday_date': '2026-04-22 20:26:28 ET', 'net_liq_delta': 5954.3, 'risk_delta': -6.5, 'regime_cha
- `17:33:58`   picks: ✗ NOT FOUND under any of ['top_picks', 'picks', 'signals', 'top_stocks', 'top_signals']
- `17:33:58`   regime: ✗ NOT FOUND under any of ['regime', 'market_regime', 'regime_info']
- `17:33:58` 
## B. EventBridge rules — detect duplicates firing same Lambda

- `17:34:08`   Lambdas with multiple EB rules pointing at them:
- `17:34:08` 
  🔴 aiapi-market-analyzer — 4 rules:
- `17:34:08`     [ENABLED] aiapi-daily-analysis  cron(0 7 * * ? *)
- `17:34:08`     [ENABLED] aiapi-hourly-predictions  rate(1 hour)
- `17:34:08`     [ENABLED] aiapi-market-data-collection  rate(15 minutes)
- `17:34:08`     [ENABLED] aiapi-weekly-training  cron(0 8 ? * SUN *)
- `17:34:08` 
  🔴 aiapi-monitor — 2 rules:
- `17:34:08`     [ENABLED] aiapi-daily-report  cron(0 14 * * ? *)
- `17:34:08`     [ENABLED] aiapi-hourly-monitor  rate(1 hour)
- `17:34:08` 
  🔴 bls-employment-api-v2 — 2 rules:
- `17:34:08`     [ENABLED] bls-friday-update  cron(0 22 ? * FRI *)
- `17:34:08`     [ENABLED] bls-tuesday-update  cron(0 22 ? * TUE *)
- `17:34:08` 
  🔴 fedapi — 2 rules:
- `17:34:08`     [ENABLED] fed-liquidity-update-monday  cron(0 14 ? * MON *)
- `17:34:08`     [ENABLED] fed-liquidity-update-thursday  cron(0 14 ? * THU *)
- `17:34:08` 
  🔴 fmp-stock-picks-agent — 3 rules:
- `17:34:08`     [ENABLED] fmp-movers-hourly  cron(0 14,16,18,20 ? * MON-FRI *)
- `17:34:08`     [ENABLED] fmp-stock-picks-daily  cron(0 12 ? * MON-FRI *)
- `17:34:08`     [ENABLED] fmp-stock-picks-daily  cron(0 12 ? * MON-FRI *)
- `17:34:08` 
  🔴 fredapi — 2 rules:
- `17:34:08`     [ENABLED] fed-liquidity-auto-update  cron(0 13 ? * MON,WED,FRI *)
- `17:34:08`     [ENABLED] fredapi-weekly-update  cron(0 14 ? * MON *)
- `17:34:08` 
  🔴 global-liquidity-agent-v2 — 8 rules:
- `17:34:08`     [ENABLED] DailyLiquidityReportRule  cron(0 12 * * ? *)
- `17:34:08`     [ENABLED] khalid-daily-report  cron(0 13 * * ? *)
- `17:34:08`     [DISABLED] liquidity-critical-monitor  rate(15 minutes)
- `17:34:08`     [DISABLED] liquidity-daily-8am  cron(0 13 * * ? *)
- `17:34:08`     [DISABLED] liquidity-daily-report  cron(0 13 * * ? *)
- `17:34:08`     [DISABLED] liquidity-daily-report-v2  cron(0 12 * * ? *)
- `17:34:08`     [DISABLED] liquidity-hourly-v2  rate(1 hour)
- `17:34:08`     [DISABLED] liquidity-news-v2  rate(15 minutes)
- `17:34:08` 
  🔴 justhodl-crypto-intel — 2 rules:
- `17:34:08`     [ENABLED] justhodl-crypto-15min  rate(15 minutes)
- `17:34:08`     [ENABLED] justhodl-crypto-intel-schedule  rate(15 minutes)
- `17:34:08` 
  🔴 justhodl-daily-report-v3 — 5 rules:
- `17:34:08`     [ENABLED] justhodl-daily-8am  cron(0 13 * * ? *)
- `17:34:08`     [ENABLED] justhodl-daily-v3  cron(0 13 * * ? *)
- `17:34:08`     [ENABLED] justhodl-v9-auto-refresh  rate(5 minutes)
- `17:34:08`     [ENABLED] justhodl-v9-evening  cron(0 23 ? * MON-FRI *)
- `17:34:08`     [ENABLED] justhodl-v9-morning  cron(0 13 ? * MON-FRI *)
- `17:34:08` 
  🔴 justhodl-intelligence — 2 rules:
- `17:34:08`     [ENABLED] justhodl-intel-daily  cron(10 12 * * ? *)
- `17:34:08`     [ENABLED] justhodl-intel-hourly  cron(5 12-23 ? * MON-FRI *)
- `17:34:08` 
  🔴 justhodl-ml-predictions — 2 rules:
- `17:34:08`     [ENABLED] justhodl-ml-predictions-schedule  rate(4 hours)
- `17:34:08`     [ENABLED] justhodl-ml-schedule  rate(4 hours)
- `17:34:08` 
  🔴 justhodl-repo-monitor — 2 rules:
- `17:34:08`     [ENABLED] justhodl-repo-30min  cron(0/30 13-23 ? * MON-FRI *)
- `17:34:08`     [ENABLED] justhodl-repo-daily  cron(0 12 * * ? *)
- `17:34:08` 
  🔴 ofrapi — 2 rules:
- `17:34:08`     [ENABLED] ofr-daily-collection  cron(0 14 * * ? *)
- `17:34:08`     [ENABLED] ofr-weekly-report  cron(0 13 ? * MON *)
- `17:34:08` 
- `17:34:08`   Lambdas with exactly 1 rule (normal): 49
## C. FRED v3.2 skip rate — last 3 runs

- `17:34:09`   Skip counts (skipped, fetched): [(207, 26), (207, 26), (207, 26), (207, 26), (207, 26)]
- `17:34:09`     → 207/233 = 89% skip
- `17:34:09`     → 207/233 = 89% skip
- `17:34:09`     → 207/233 = 89% skip
- `17:34:09`     → 207/233 = 89% skip
- `17:34:09`     → 207/233 = 89% skip
- `17:34:09` ✅   ✓ Healthy: 89% skip on latest run
- `17:34:09` 
  FRED fetch times: ['13.0s', '23.6s', '16.4s', '33.4s', '13.1s']
- `17:34:09`   Average: 19.9s
- `17:34:09`   End-to-end DONE: ['99.5s', '142.5s', '112.4s', '111.5s', '102.3s']
## D. Frontend — justhodl.ai pages (indirect via S3 ref probe)

- `17:34:09`   DNS: justhodl.ai → 185.199.109.153 (185.x = GitHub Pages ✓)
- `17:34:09`   DNS: api.justhodl.ai → 104.21.71.253 (Cloudflare range ✓)
- `17:34:09` Done
