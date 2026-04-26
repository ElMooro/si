# Path D — what does each silent Lambda produce?

**Status:** success  
**Duration:** 32.1s  
**Finished:** 2026-04-26T01:04:40+00:00  

## Log
## A. justhodl-* Lambda inventory

- `01:04:08`   48 justhodl-* Lambdas total
## B. Schedule lookup

## C. S3 inventory

- `01:04:15`   19362 S3 objects total
## D. Per-Lambda audit

- `01:04:16` 
  ── justhodl-asymmetric-scorer  [✓]
- `01:04:16`      schedule: cron(30 13 ? * MON-FRI *)
- `01:04:16`      log status: 0 keys logged
- `01:04:19` 
  ── justhodl-bloomberg-v8  [✗]
- `01:04:19`      schedule: rate(5 minutes)
- `01:04:19`      log status: 0 keys logged
- `01:04:20` 
  ── justhodl-bond-regime-detector  [✓]
- `01:04:20`      schedule: cron(0 */4 * * ? *)
- `01:04:20`      log status: 0 keys logged
- `01:04:20` 
  ── justhodl-calibrator  [✓]
- `01:04:20`      schedule: cron(0 9 ? * SUN *)
- `01:04:20`      log status: 0 keys logged
- `01:04:20` 
  ── justhodl-cot-extremes-scanner  [✓]
- `01:04:20`      schedule: cron(0 19 ? * FRI *)
- `01:04:20`      log status: 0 keys logged
- `01:04:20` 
  ── justhodl-crypto-enricher  [✗]
- `01:04:20`      schedule: cron(15 6 * * ? *)
- `01:04:20`      log status: 0 keys logged
- `01:04:21` 
  ── justhodl-crypto-intel  [✓]
- `01:04:21`      schedule: rate(15 minutes)
- `01:04:21`      log status: 0 keys logged
- `01:04:21` 
  ── justhodl-daily-macro-report  [✗]
- `01:04:21`      schedule: cron(0 12 * * ? *)
- `01:04:21`      log status: NO LOGS
- `01:04:24` 
  ── justhodl-daily-report-v3  [✓]
- `01:04:24`      schedule: rate(5 minutes); cron(0 23 ? * MON-FRI *); cron(0 13 ? * MON-FRI *)
- `01:04:24`      log status: 0 keys logged
- `01:04:25` 
  ── justhodl-dex-scanner  [✓]
- `01:04:25`      schedule: rate(15 minutes)
- `01:04:25`      log status: 0 keys logged
- `01:04:26` 
  ── justhodl-divergence-scanner  [✓]
- `01:04:26`      schedule: cron(0 13 ? * MON-FRI *)
- `01:04:26`      log status: 0 keys logged
- `01:04:26` 
  ── justhodl-edge-engine  [✓]
- `01:04:26`      schedule: rate(6 hours)
- `01:04:26`      log status: 0 keys logged
- `01:04:26` 
  ── justhodl-email-reports  [✗]
- `01:04:26`      schedule: cron(0 13 * * ? *)
- `01:04:26`      log status: 0 keys logged
- `01:04:26` 
  ── justhodl-email-reports-v2  [✗]
- `01:04:26`      schedule: cron(0 12 * * ? *)
- `01:04:26`      log status: 0 keys logged
- `01:04:27` 
  ── justhodl-financial-secretary  [✗]
- `01:04:27`      schedule: rate(4 hours)
- `01:04:27`      log status: 0 keys logged
- `01:04:27` 
  ── justhodl-health-monitor  [✗]
- `01:04:27`      schedule: cron(0/15 * * * ? *)
- `01:04:27`      log status: 0 keys logged
- `01:04:28` 
  ── justhodl-intelligence  [✓]
- `01:04:28`      schedule: cron(10 12 * * ? *); cron(5 12-23 ? * MON-FRI *)
- `01:04:28`      log status: 0 keys logged
- `01:04:29` 
  ── justhodl-khalid-metrics  [✓]
- `01:04:29`      schedule: cron(0 11 * * ? *)
- `01:04:29`      log status: 0 keys logged
- `01:04:29` 
  ── justhodl-liquidity-agent  [✓]
- `01:04:29`      schedule: cron(30 12 * * ? *)
- `01:04:29`      log status: 1 keys logged
- `01:04:29`      🟢 liquidity-data.json                                  12.6h ago
- `01:04:29` 
  ── justhodl-ml-predictions  [✓]
- `01:04:29`      schedule: rate(4 hours)
- `01:04:29`      log status: 0 keys logged
- `01:04:30` 
  ── justhodl-morning-intelligence  [✓]
- `01:04:30`      schedule: cron(0 13 * * ? *)
- `01:04:30`      log status: 0 keys logged
- `01:04:31` 
  ── justhodl-news-sentiment  [✗]
- `01:04:31`      schedule: cron(15 6 * * ? *)
- `01:04:31`      log status: 0 keys logged
- `01:04:33` 
  ── justhodl-options-flow  [✓]
- `01:04:33`      schedule: rate(5 minutes)
- `01:04:33`      log status: 0 keys logged
- `01:04:34` 
  ── justhodl-outcome-checker  [✓]
- `01:04:34`      schedule: cron(30 22 ? * MON-FRI *); cron(0 8 1 * ? *); cron(0 8 ? * SUN *)
- `01:04:34`      log status: 0 keys logged
- `01:04:35` 
  ── justhodl-pnl-tracker  [✓]
- `01:04:35`      schedule: cron(0 22 * * ? *)
- `01:04:35`      log status: 1 keys logged
- `01:04:35`      🟢 portfolio/pnl-daily.json                              3.1h ago
- `01:04:35` 
  ── justhodl-prompt-iterator  [✓]
- `01:04:35`      schedule: cron(0 10 ? * SUN *)
- `01:04:35`      log status: 0 keys logged
- `01:04:35` 
  ── justhodl-repo-monitor  [✗]
- `01:04:35`      schedule: cron(0/30 13-23 ? * MON-FRI *); cron(0 12 * * ? *)
- `01:04:35`      log status: 0 keys logged
- `01:04:36` 
  ── justhodl-reports-builder  [✓]
- `01:04:36`      schedule: rate(1 hour)
- `01:04:36`      log status: 0 keys logged
- `01:04:36` 
  ── justhodl-risk-sizer  [✓]
- `01:04:36`      schedule: cron(45 13 ? * MON-FRI *)
- `01:04:36`      log status: 0 keys logged
- `01:04:37` 
  ── justhodl-signal-logger  [✓]
- `01:04:37`      schedule: rate(6 hours)
- `01:04:37`      log status: 0 keys logged
- `01:04:39` 
  ── justhodl-stock-screener  [✓]
- `01:04:39`      schedule: rate(4 hours)
- `01:04:39`      log status: 0 keys logged
- `01:04:39` 
  ── justhodl-telegram-bot  [✓]
- `01:04:39`      schedule: rate(2 hours)
- `01:04:39`      log status: 0 keys logged
- `01:04:40` 
  ── justhodl-valuations-agent  [✓]
- `01:04:40`      schedule: cron(0 14 1 * ? *)
- `01:04:40`      log status: 0 keys logged
- `01:04:40` 
  ── justhodl-watchlist-debate  [✓]
- `01:04:40`      schedule: cron(0 3 * * ? *)
- `01:04:40`      log status: NO LOGS
## E. Coverage gaps — Lambdas producing data NOT on website

- `01:04:40` 
  0 Lambdas producing untouched data:
## F. Summary

- `01:04:40` 
  Total justhodl-* Lambdas:        48
- `01:04:40`   Surfaced on website:             32/48
- `01:04:40`   Producing S3 data (audited):     2
- `01:04:40`   Coverage gaps (have data, no UI): 0
- `01:04:40` Done
