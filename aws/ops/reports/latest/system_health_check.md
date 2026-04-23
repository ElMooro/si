# JustHodl.AI — End-to-End System Health Check

**Status:** success  
**Duration:** 18.0s  
**Finished:** 2026-04-23T17:31:51+00:00  

## Data

| age_min | ai_chat_errors_6h | ai_chat_invocations_6h | crypto | fred_count | hit_rate | khalid_index | lambdas_alive | lambdas_stale | lambdas_total | pipeline | regime | s3_public | secretary_age_min | size_mb | stocks | tier2_crypto | tier2_options |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.1 |  |  |  |  |  |  |  |  |  | FRESH |  |  |  | 1.64 |  |  |  |
|  |  |  | 25 | 233 |  | 43 |  |  |  |  | BEAR |  |  |  | 187 |  |  |
|  |  |  |  |  | False |  |  |  |  |  |  |  | 5.0 |  |  | False | False |
|  |  |  |  |  |  |  | 11 | 0 | 12 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 5/5 |  |  |  |  |  |
|  | 0 | 8 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |

## Log
## 1. Data pipeline — data/report.json freshness

- `17:31:33`   Last modified: 2026-04-23T17:30:26+00:00 (1.1 min ago)
- `17:31:33`   Size: 1.64 MB
- `17:31:33` ✅   ✓ FRESH — within 10 min (schedule is 5 min)
- `17:31:33` 
  Top-level keys: ['ai_analysis', 'ath_breakouts', 'cftc_positioning', 'crypto', 'crypto_global', 'ecb_ciss', 'fetch_time_seconds', 'fred', 'generated_at', 'khalid_index']…
- `17:31:33`   Khalid Index: score=43, regime=BEAR
- `17:31:33`   FRED series in report: 233
- `17:31:33`   Stocks tracked: 187
- `17:31:33`   Crypto coins: 25
## 2. FRED cache v3.2 — still hitting high skip rate?

- `17:31:34`   fred-cache.json: 1,022,825 bytes, 2.5 min old
- `17:31:35` ⚠   FRED cache check failed: name 're' is not defined
## 3. Secretary v2.2 — 4h brief + tier-2 cards

- `17:31:35`   secretary-latest.json: 136,520 bytes, 5.0 min old
- `17:31:35` ✅   ✓ Within 4h schedule window
- `17:31:35`   v2.2 features present:
- `17:31:35`     options_flow card: False
- `17:31:35`     crypto_intel card: False
- `17:31:35`     sector rotation: False
- `17:31:35`     hit rate tracking: False
- `17:31:35`     top_inflow_flow value: None (should be <$1B after $M format fix)
## 4. Critical Lambdas — alive + recent activity

- `17:31:35`   ✓ justhodl-daily-report-v3               ALIVE  last_inv     6m ago, 3 invs
- `17:31:36`   ✓ justhodl-financial-secretary           ALIVE  last_inv    11m ago, 8 invs [1 err]
- `17:31:38`   ✓ justhodl-ai-chat                       ALIVE  last_inv     6m ago, 18 invs
- `17:31:39`   ✓ cftc-futures-positioning-agent         ALIVE  last_inv     6m ago, 321 invs
- `17:31:39`   ? justhodl-stock-analyzer                NO INVOCATIONS in window  (expected_cadence=Nonem)
- `17:31:40`   ✓ justhodl-stock-screener                ALIVE  last_inv   126m ago, 3 invs
- `17:31:41`   ✓ justhodl-edge-engine                   ALIVE  last_inv    91m ago, 4 invs
- `17:31:42`   ✓ justhodl-morning-intelligence          ALIVE  last_inv   276m ago, 5 invs [1 err]
- `17:31:43`   ✓ justhodl-investor-agents               ALIVE  last_inv  1081m ago, 2 invs
- `17:31:44`   ✓ justhodl-telegram-bot                  ALIVE  last_inv    31m ago, 12 invs
- `17:31:45`   ✓ justhodl-signal-logger                 ALIVE  last_inv   146m ago, 5 invs [1 err]
- `17:31:46`   ✓ justhodl-dex-scanner                   ALIVE  last_inv     6m ago, 96 invs
## 5. S3 public HTTPS access (dashboard readability)

- `17:31:46`   ✓ data/report.json: 200 OK
- `17:31:46`   ✓ data/secretary-latest.json: 200 OK
- `17:31:46`   ✓ data/fred-cache.json: 200 OK
- `17:31:46`   ✓ flow-data.json: 200 OK
- `17:31:46`   ✓ crypto-intel.json: 200 OK
## 6. AI chat — api.justhodl.ai reachability

- `17:31:46`   NOTE: sandbox egress can't reach api.justhodl.ai directly.
- `17:31:46`   Indirect check: is the underlying Lambda invoking + returning 200s?
- `17:31:46`   justhodl-ai-chat — last 6h: 8 invocations, 0 errors
- `17:31:46` ✅   ✓ Healthy — 0.0% error rate
## 7. EventBridge schedule rules

- `17:31:47`   Found 29 justhodl-prefix EB rules:
- `17:31:47`     [ENABLED] justhodl-8am                                       cron(0 13 * * ? *)
- `17:31:47`     [ENABLED] justhodl-calibrator-weekly                         cron(0 9 ? * SUN *)
- `17:31:47`     [ENABLED] justhodl-crypto-15min                              rate(15 minutes)
- `17:31:47`     [ENABLED] justhodl-crypto-enricher-daily                     cron(15 6 * * ? *)
- `17:31:47`     [ENABLED] justhodl-crypto-intel-schedule                     rate(15 minutes)
- `17:31:47`     [ENABLED] justhodl-daily-8am                                 cron(0 13 * * ? *)
- `17:31:47`     [ENABLED] justhodl-daily-v3                                  cron(0 13 * * ? *)
- `17:31:47`     [ENABLED] justhodl-dex-scanner-15min                         rate(15 minutes)
- `17:31:47`     [ENABLED] justhodl-edge-6h                                   rate(6 hours)
- `17:31:47`     [ENABLED] justhodl-edge-engine-6h                            rate(6 hours)
- `17:31:47`     [ENABLED] justhodl-flow-refresh                              rate(5 minutes)
- `17:31:47`     [ENABLED] justhodl-hourly-collection                         rate(1 hour)
- `17:31:47`     [ENABLED] justhodl-intel-daily                               cron(10 12 * * ? *)
- `17:31:47`     [ENABLED] justhodl-intel-hourly                              cron(5 12-23 ? * MON-FRI *)
- `17:31:47`     [ENABLED] justhodl-khalid-metrics-refresh                    cron(0 11 * * ? *)
- `17:31:47`     [ENABLED] justhodl-liquidity-agent-daily                     cron(30 12 * * ? *)
- `17:31:47`     [ENABLED] justhodl-ml-predictions-schedule                   rate(4 hours)
- `17:31:47`     [ENABLED] justhodl-ml-schedule                               rate(4 hours)
- `17:31:47`     [ENABLED] justhodl-morning-brief-daily                       cron(0 13 * * ? *)
- `17:31:47`     [ENABLED] justhodl-outcome-checker-weekly                    cron(0 8 ? * SUN *)
- `17:31:47`     [ENABLED] justhodl-repo-30min                                cron(0/30 13-23 ? * MON-FRI *)
- `17:31:47`     [ENABLED] justhodl-repo-daily                                cron(0 12 * * ? *)
- `17:31:47`     [ENABLED] justhodl-sentiment-daily                           cron(15 6 * * ? *)
- `17:31:47`     [ENABLED] justhodl-signal-logger-6h                          rate(6 hours)
- `17:31:47`     [ENABLED] justhodl-stock-screener-4h                         rate(4 hours)
- `17:31:47`     [ENABLED] justhodl-telegram-alerts                           rate(2 hours)
- `17:31:47`     [ENABLED] justhodl-v9-auto-refresh                           rate(5 minutes)
- `17:31:47`     [ENABLED] justhodl-v9-evening                                cron(0 23 ? * MON-FRI *)
- `17:31:47`     [ENABLED] justhodl-v9-morning                                cron(0 13 ? * MON-FRI *)
- `17:31:47` 
- `17:31:49`     [ENABLED] cftc-cot-weekly-update → cftc-futures-positioning-agent  cron(0 18 ? * FRI *)
- `17:31:51`     [ENABLED] secretary-4h-scan → justhodl-financial-secretary  rate(4 hours)
## 8. Summary

- `17:31:51`   See all sections above. Any line starting with ✗ is a real failure.
- `17:31:51`   ⚠ means needs attention but not immediately broken.
- `17:31:51`   ✓ means verified working.
- `17:31:51` Done
