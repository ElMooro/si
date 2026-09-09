# ops 5250 — Lambda recursion forensics + cost/runaway guardrails

**Status:** success  
**Duration:** 126.4s  
**Finished:** 2026-09-09T13:18:11+00:00  

## Data

| cloudwatch | day | dropped | first | fn | function | gb_s | inv | kind | lambda_ | last | own_cap | reserved_concurrency | risk | s3 | section | total | triggers | tripped_14d | usd_7d |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | 6 | 2026-08-31T04:11:00+00:00 |  | justhodl-worldbank-full |  |  |  |  | 2026-09-07T11:26:00+00:00 |  |  |  |  | tripped |  |  |  |  |
|  |  |  |  | justhodl-13f-clone-alpha |  |  |  | WALK |  |  | 10 | None | capped<16 |  | census |  | scheduler:justhodl-13f-clone-alpha-weekly cron(30 8 ? * MON *) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-bls-full |  |  |  | WALK |  |  | 80 | None | BREAKS@16 |  | census |  | scheduler:justhodl-bls-full-12h rate(24 hours) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-boj-full |  |  |  | FANOUT/ONE-SHOT |  |  | None | None | n/a |  | census |  | scheduler:justhodl-boj-full-fanout rate(30 minutes) [ENABLED]; scheduler:justhodl-boj-full-12h rate(24 hours) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-census-us |  |  |  | FANOUT/ONE-SHOT |  |  | None | 20 | n/a |  | census |  | scheduler:justhodl-census-us-15min rate(15 minutes) [ENABLED]; scheduler:justhodl-census-econ-dispatch rate(15 minutes) [ENABLED]; rule:carry-surface-4h rate(4 hours) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-ecb-deep |  |  |  | WALK |  |  | 60 | None | BREAKS@16 |  | census |  | scheduler:justhodl-ecb-deep-10min rate(6 hours) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-equity-research |  |  |  | WALK |  |  | None | None | BREAKS@16 |  | census |  |  | 0 |  |
|  |  |  |  | justhodl-finra-full |  |  |  | WALK |  |  | 40 | None | BREAKS@16 |  | census |  | scheduler:justhodl-finra-full-6h rate(24 hours) [ENABLED]; scheduler:justhodl-finra-full-weekly rate(7 days) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-fiscaldata-full |  |  |  | WALK |  |  | 60 | None | BREAKS@16 |  | census |  | scheduler:justhodl-fiscaldata-full-weekly rate(7 days) [ENABLED]; scheduler:justhodl-fiscaldata-full-2h rate(24 hours) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-fleet-integrity |  |  |  | WALK |  |  | 12 | None | capped<16 |  | census |  | rule:justhodl-d1-scan-daily cron(0 5 * * ? *) [ENABLED]; rule:justhodl-fleet-integrity-weekly cron(0 8 ? * MON *) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-fred-catalog |  |  |  | WALK |  |  | None | 1 | BREAKS@16 |  | census |  | rule:justhodl-fred-catalog-5min rate(15 minutes) [ENABLED]; rule:justhodl-fred-catalog-5min rate(15 minutes) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-fundamental-census |  |  |  | WALK |  |  | 12 | None | capped<16 |  | census |  | scheduler:fundamental-census-sched cron(0 6 1,15 * ? *) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-gdelt-full |  |  |  | WALK |  |  | 220 | None | BREAKS@16 |  | census |  | scheduler:justhodl-gdelt-full-30m rate(6 hours) [ENABLED]; rule:benzinga-news-agent-warm rate(5 minutes) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-hist-banker |  |  |  | WALK |  |  | 60 | None | BREAKS@16 |  | census |  | scheduler:justhodl-hist-banker-weekly cron(45 4 ? * WED *) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-imf-full |  |  |  | WALK |  |  | 40 | None | BREAKS@16 |  | census |  | scheduler:justhodl-imf-full-weekly rate(7 days) [ENABLED]; scheduler:justhodl-imf-full-6h rate(24 hours) [DISABLED] | 0 |  |
|  |  |  |  | justhodl-polygon-full |  |  |  | WALK |  |  | 30 | None | BREAKS@16 |  | census |  | scheduler:justhodl-polygon-full-2h rate(24 hours) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-sdmx-walker |  |  |  | FANOUT/ONE-SHOT |  |  | None | None | n/a |  | census |  | scheduler:justhodl-statcan-retry-hourly rate(24 hours) [ENABLED]; scheduler:justhodl-ecb-rewalk-weekly cron(15 3 ? * SUN *) [ENABLED]; scheduler:justhodl-oecd-retry-hourly rate(15 minutes) [ENABLED]; rule:justhodl-sdmx-walker-hourly rate(5 minutes) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-sec-midas |  |  |  | WALK |  |  | 40 | None | BREAKS@16 |  | census |  | scheduler:justhodl-sec-midas-weekly rate(24 hours) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-symdir |  |  |  | FANOUT/ONE-SHOT |  |  | None | None | n/a |  | census |  | scheduler:justhodl-symdir-ustbank cron(30 21 ? * MON-FRI *) [ENABLED]; scheduler:justhodl-symdir-build cron(40 5 * * ? *) [ENABLED]; scheduler:justhodl-symdir-codelists rate(20 minutes) [ENABLED]; scheduler:justhodl-symdir-fredfresh rate(1 hour) [ENABLED]; scheduler:justhodl-symdir-fredupdates rate(15 minutes) [ENABLED]; scheduler:justhodl-symdir-warm rate(5 minutes) [ENABLED]; scheduler:justhodl-symdir-titles rate(1 hour) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-trend-reversal |  |  |  | WALK |  |  | None | None | BREAKS@16 |  | census |  | scheduler:trend-reversal-daily cron(50 20 ? * MON-FRI *) [ENABLED] | 0 |  |
|  |  |  |  | justhodl-tv-bars |  |  |  | FANOUT/ONE-SHOT |  |  | None | None | n/a |  | census |  | scheduler:justhodl-tv-bars-universe-refresh cron(30 2 * * ? *) [ENABLED]; rule:justhodl-tv-bars-hourly rate(1 hour) [DISABLED] | 0 |  |
|  |  |  |  | justhodl-worldbank-full |  |  |  | WALK |  |  | 140 | None | BREAKS@16 |  | census |  | scheduler:justhodl-worldbank-full-weekly rate(7 days) [ENABLED]; scheduler:justhodl-worldbank-full-2h rate(2 hours) [ENABLED] | 6 |  |
| 0.03 | 2026-09-01 |  |  |  |  |  |  |  | 9.26 |  |  |  |  | 6.99 | ce_daily | 18.16 |  |  |  |
| 0.08 | 2026-09-02 |  |  |  |  |  |  |  | 17.4 |  |  |  |  | 6.84 | ce_daily | 24.7 |  |  |  |
| 0.11 | 2026-09-03 |  |  |  |  |  |  |  | 9.7 |  |  |  |  | 5.95 | ce_daily | 16.16 |  |  |  |
| 0.04 | 2026-09-04 |  |  |  |  |  |  |  | 3.03 |  |  |  |  | 11.89 | ce_daily | 15.28 |  |  |  |
| 0.04 | 2026-09-05 |  |  |  |  |  |  |  | 2.83 |  |  |  |  | 4.93 | ce_daily | 8.12 |  |  |  |
| 0.04 | 2026-09-06 |  |  |  |  |  |  |  | 2.51 |  |  |  |  | 5.1 | ce_daily | 7.97 |  |  |  |
| 0.07 | 2026-09-07 |  |  |  |  |  |  |  | 3.09 |  |  |  |  | 4.06 | ce_daily | 7.63 |  |  |  |
| 0.04 | 2026-09-08 |  |  |  |  |  |  |  | 2.34 |  |  |  |  | 4.2 | ce_daily | 7.09 |  |  |  |
|  |  |  |  |  | justhodl-sdmx-walker | 115725 | 13068 |  |  |  |  |  |  |  | top_inv |  |  |  | 1.93 |
|  |  |  |  |  | justhodl-symdir | 358852 | 8457 |  |  |  |  |  |  |  | top_inv |  |  |  | 5.98 |
|  |  |  |  |  | justhodl-boj-full | 219064 | 7735 |  |  |  |  |  |  |  | top_inv |  |  |  | 3.65 |
|  |  |  |  |  | justhodl-a2a-bus | 11523 | 6600 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.19 |
|  |  |  |  |  | justhodl-equity-research | 12944 | 6538 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.22 |
|  |  |  |  |  | justhodl-census-us | 344803 | 5954 |  |  |  |  |  |  |  | top_inv |  |  |  | 5.75 |
|  |  |  |  |  | justhodl-wl-series-api | 17 | 4340 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.0 |
|  |  |  |  |  | justhodl-streaming-fanout | 1207 | 2400 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.02 |
|  |  |  |  |  | justhodl-market-tape | 2085 | 2115 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.04 |
|  |  |  |  |  | justhodl-gdelt-full | 467 | 2045 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.01 |
|  |  |  |  |  | benzinga-news-agent | 470 | 2038 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.01 |
|  |  |  |  |  | justhodl-edgar-insiders | 2251 | 1969 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.04 |
|  |  |  |  |  | justhodl-event-coordinator | 40 | 1853 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.0 |
|  |  |  |  |  | justhodl-research-critique | 3670 | 1800 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.06 |
|  |  |  |  |  | justhodl-fundamental-graphs | 4288 | 1762 |  |  |  |  |  |  |  | top_inv |  |  |  | 0.07 |
|  |  |  |  |  | justhodl-symdir | 358852 | 8457 |  |  |  |  |  |  |  | top_usd |  |  |  | 5.98 |
|  |  |  |  |  | justhodl-census-us | 344803 | 5954 |  |  |  |  |  |  |  | top_usd |  |  |  | 5.75 |
|  |  |  |  |  | justhodl-boj-full | 219064 | 7735 |  |  |  |  |  |  |  | top_usd |  |  |  | 3.65 |
|  |  |  |  |  | justhodl-repo | 185472 | 357 |  |  |  |  |  |  |  | top_usd |  |  |  | 3.09 |
|  |  |  |  |  | justhodl-sdmx-walker | 115725 | 13068 |  |  |  |  |  |  |  | top_usd |  |  |  | 1.93 |
|  |  |  |  |  | justhodl-provider-catalog | 67216 | 173 |  |  |  |  |  |  |  | top_usd |  |  |  | 1.12 |
|  |  |  |  |  | justhodl-ecb-deep | 65566 | 203 |  |  |  |  |  |  |  | top_usd |  |  |  | 1.09 |
|  |  |  |  |  | justhodl-news-velocity | 54355 | 168 |  |  |  |  |  |  |  | top_usd |  |  |  | 0.91 |
|  |  |  |  |  | justhodl-research-backtest | 49882 | 29 |  |  |  |  |  |  |  | top_usd |  |  |  | 0.83 |
|  |  |  |  |  | justhodl-katlin | 36858 | 23 |  |  |  |  |  |  |  | top_usd |  |  |  | 0.61 |
|  |  |  |  |  | justhodl-worldbank-full | 36368 | 137 |  |  |  |  |  |  |  | top_usd |  |  |  | 0.61 |
|  |  |  |  |  | justhodl-13f-positions | 26392 | 84 |  |  |  |  |  |  |  | top_usd |  |  |  | 0.44 |
|  |  |  |  |  | justhodl-repo-monitor | 16905 | 345 |  |  |  |  |  |  |  | top_usd |  |  |  | 0.28 |
|  |  |  |  |  | justhodl-backend-agent | 14482 | 672 |  |  |  |  |  |  |  | top_usd |  |  |  | 0.24 |
|  |  |  |  |  | justhodl-risk-gate | 14297 | 182 |  |  |  |  |  |  |  | top_usd |  |  |  | 0.24 |

## Log
## A. WHO tripped AWS's recursion breaker (RecursiveInvocationsDropped, 14d)

- `13:16:05` functions that have EVER published the metric (≤2w window): 1
- `13:16:05` ✗   TRIPPED justhodl-worldbank-full                      dropped=6  first=08-31 04:11  last=09-07 11:26
- `13:16:05`       2026-08-31 04:11 UTC  +1
- `13:16:05`       2026-08-31 07:26 UTC  +1
- `13:16:05`       2026-08-31 11:26 UTC  +1
- `13:16:05`       2026-09-07 04:11 UTC  +1
- `13:16:05`       2026-09-07 07:26 UTC  +1
- `13:16:05`       2026-09-07 11:26 UTC  +1
- `13:16:06` account-level aggregate (no dimension) daily: 08-30=3, 09-06=3
- `13:16:06` Health API unavailable (An error occurred (SubscriptionRequiredException) when calling the DescribeEvents operatio) — metric evidence above is authoritative
## B. Chain census — every engine that Event-invokes ITSELF (static, this checkout)

- `13:17:13`   justhodl-13f-clone-alpha           WALK             cap=10    capped<16  rc=None trips=0   scheduler:justhodl-13f-clone-alpha-weekly cron(30 8 ? * MON *) [ENABLE
- `13:17:14` ⚠   justhodl-bls-full                  WALK             cap=80    BREAKS@16  rc=None trips=0   scheduler:justhodl-bls-full-12h rate(24 hours) [ENABLED]
- `13:17:14`   justhodl-boj-full                  FANOUT/ONE-SHOT  cap=None  n/a        rc=None trips=0   scheduler:justhodl-boj-full-fanout rate(30 minutes) [ENABLED]; schedul
- `13:17:14`   justhodl-census-us                 FANOUT/ONE-SHOT  cap=None  n/a        rc=20   trips=0   scheduler:justhodl-census-us-15min rate(15 minutes) [ENABLED]; schedul
- `13:17:14` ⚠   justhodl-ecb-deep                  WALK             cap=60    BREAKS@16  rc=None trips=0   scheduler:justhodl-ecb-deep-10min rate(6 hours) [ENABLED]
- `13:17:14` ⚠   justhodl-equity-research           WALK             cap=None  BREAKS@16  rc=None trips=0   NO TRIGGER
- `13:17:14` ⚠   justhodl-finra-full                WALK             cap=40    BREAKS@16  rc=None trips=0   scheduler:justhodl-finra-full-6h rate(24 hours) [ENABLED]; scheduler:j
- `13:17:14` ⚠   justhodl-fiscaldata-full           WALK             cap=60    BREAKS@16  rc=None trips=0   scheduler:justhodl-fiscaldata-full-weekly rate(7 days) [ENABLED]; sche
- `13:17:14`   justhodl-fleet-integrity           WALK             cap=12    capped<16  rc=None trips=0   rule:justhodl-d1-scan-daily cron(0 5 * * ? *) [ENABLED]; rule:justhodl
- `13:17:14` ⚠   justhodl-fred-catalog              WALK             cap=None  BREAKS@16  rc=1    trips=0   rule:justhodl-fred-catalog-5min rate(15 minutes) [ENABLED]; rule:justh
- `13:17:14`   justhodl-fundamental-census        WALK             cap=12    capped<16  rc=None trips=0   scheduler:fundamental-census-sched cron(0 6 1,15 * ? *) [ENABLED]
- `13:17:15` ⚠   justhodl-gdelt-full                WALK             cap=220   BREAKS@16  rc=None trips=0   scheduler:justhodl-gdelt-full-30m rate(6 hours) [ENABLED]; rule:benzin
- `13:17:15` ⚠   justhodl-hist-banker               WALK             cap=60    BREAKS@16  rc=None trips=0   scheduler:justhodl-hist-banker-weekly cron(45 4 ? * WED *) [ENABLED]
- `13:17:15` ⚠   justhodl-imf-full                  WALK             cap=40    BREAKS@16  rc=None trips=0   scheduler:justhodl-imf-full-weekly rate(7 days) [ENABLED]; scheduler:j
- `13:17:15` ⚠   justhodl-polygon-full              WALK             cap=30    BREAKS@16  rc=None trips=0   scheduler:justhodl-polygon-full-2h rate(24 hours) [ENABLED]
- `13:17:15`   justhodl-sdmx-walker               FANOUT/ONE-SHOT  cap=None  n/a        rc=None trips=0   scheduler:justhodl-statcan-retry-hourly rate(24 hours) [ENABLED]; sche
- `13:17:15` ⚠   justhodl-sec-midas                 WALK             cap=40    BREAKS@16  rc=None trips=0   scheduler:justhodl-sec-midas-weekly rate(24 hours) [ENABLED]
- `13:17:15`   justhodl-symdir                    FANOUT/ONE-SHOT  cap=None  n/a        rc=None trips=0   scheduler:justhodl-symdir-ustbank cron(30 21 ? * MON-FRI *) [ENABLED];
- `13:17:15` ⚠   justhodl-trend-reversal            WALK             cap=None  BREAKS@16  rc=None trips=0   scheduler:trend-reversal-daily cron(50 20 ? * MON-FRI *) [ENABLED]
- `13:17:15`   justhodl-tv-bars                   FANOUT/ONE-SHOT  cap=None  n/a        rc=None trips=0   scheduler:justhodl-tv-bars-universe-refresh cron(30 2 * * ? *) [ENABLE
- `13:17:15` ⚠   justhodl-worldbank-full            WALK             cap=140   BREAKS@16  rc=None trips=6   scheduler:justhodl-worldbank-full-weekly rate(7 days) [ENABLED]; sched
- `13:17:15` engines self-chaining a WALK with no cap or a cap above AWS's 16: 13
## C. Each tripped function — config, async config, triggers, hourly metrics, logs at the drop

- `13:17:15` ── justhodl-worldbank-full
- `13:17:16`    mem=1024MB timeout=850s modified=2026-08-24T01:12:48.000+0000
- `13:17:16`    reserved concurrency: None 
- `13:17:16`    recursion config: Terminate
- `13:17:16`    async config: default (retries=2, no destination) — An error occurred (ResourceNotFoundException) when calling t
- `13:17:16`    trigger scheduler:justhodl-worldbank-full-weekly rate(7 days) [ENABLED]
- `13:17:16`    trigger scheduler:justhodl-worldbank-full-2h rate(2 hours) [ENABLED]
- `13:17:18`    since Sep 1: invocations=156 errors=0 throttles=0 GB-s=36386 ≈ $0.61
- `13:17:18`      busiest hour 09-07 02:00 UTC  inv=7 err=0 thr=0
- `13:17:18`      busiest hour 09-07 06:00 UTC  inv=7 err=0 thr=0
- `13:17:18`      busiest hour 09-07 10:00 UTC  inv=7 err=0 thr=0
- `13:17:18`      busiest hour 09-07 03:00 UTC  inv=5 err=0 thr=0
- `13:17:18`      busiest hour 09-07 05:00 UTC  inv=5 err=0 thr=0
- `13:17:18`      busiest hour 09-07 09:00 UTC  inv=5 err=0 thr=0
## D. Fleet posture — Cost Explorer daily (Sep 1→today) and Lambda burn

- `13:17:19` day           TOTAL   Lambda       S3       CW  next-biggest
- `13:17:19` 2026-09-01    18.16     9.26     6.99     0.03  53=1.50 Manager=0.12
- `13:17:19` 2026-09-02    24.70    17.40     6.84     0.08  DynamoDB=0.13 Manager=0.12
- `13:17:19` 2026-09-03    16.16     9.70     5.95     0.11  Manager=0.12 DynamoDB=0.07
- `13:17:19` 2026-09-04    15.28     3.03    11.89     0.04  Manager=0.12 DynamoDB=0.07
- `13:17:19` 2026-09-05     8.12     2.83     4.93     0.04  Manager=0.12 DynamoDB=0.07
- `13:17:19` 2026-09-06     7.97     2.51     5.10     0.04  Manager=0.12 DynamoDB=0.08
- `13:17:19` 2026-09-07     7.63     3.09     4.06     0.07  DynamoDB=0.15 Manager=0.12
- `13:17:19` 2026-09-08     7.09     2.34     4.20     0.04  DynamoDB=0.27 Manager=0.12
- `13:17:19` run-rate (last 3 full days): $7.90/day ≈ $237/month
- `13:17:21` account hourly Invocations  9d: p95=1445 max=2069  → alarm threshold 20000/h
- `13:17:21` account hourly Errors       9d: p95=834 max=893  → alarm threshold 2502/h
- `13:17:21` account ConcurrentExecutions 9d: p95=30 max=414 → alarm threshold 150
- `13:17:21` daily invocations (last 9d): 33901, 25329, 16691, 14780, 14865, 14280, 12913, 14031, 17153
- `13:17:59` fleet: 884 functions, 832 active in 7d, Lambda compute 7d ≈ $31.61 (≈ $135/mo)
- `13:17:59` TOP by invocations (7d):
- `13:17:59`    justhodl-sdmx-walker                         inv=13068    GB-s=115725   $1.93
- `13:17:59`    justhodl-symdir                              inv=8457     GB-s=358852   $5.98
- `13:17:59`    justhodl-boj-full                            inv=7735     GB-s=219064   $3.65
- `13:17:59`    justhodl-a2a-bus                             inv=6600     GB-s=11523    $0.19
- `13:17:59`    justhodl-equity-research                     inv=6538     GB-s=12944    $0.22
- `13:17:59`    justhodl-census-us                           inv=5954     GB-s=344803   $5.75
- `13:17:59`    justhodl-wl-series-api                       inv=4340     GB-s=17       $0.00
- `13:17:59`    justhodl-streaming-fanout                    inv=2400     GB-s=1207     $0.02
- `13:17:59`    justhodl-market-tape                         inv=2115     GB-s=2085     $0.04
- `13:17:59`    justhodl-gdelt-full                          inv=2045     GB-s=467      $0.01
- `13:17:59`    benzinga-news-agent                          inv=2038     GB-s=470      $0.01
- `13:17:59`    justhodl-edgar-insiders                      inv=1969     GB-s=2251     $0.04
- `13:17:59`    justhodl-event-coordinator                   inv=1853     GB-s=40       $0.00
- `13:17:59`    justhodl-research-critique                   inv=1800     GB-s=3670     $0.06
- `13:17:59`    justhodl-fundamental-graphs                  inv=1762     GB-s=4288     $0.07
- `13:17:59` TOP by $ (7d):
- `13:17:59`    justhodl-symdir                              inv=8457     GB-s=358852   $5.98
- `13:17:59`    justhodl-census-us                           inv=5954     GB-s=344803   $5.75
- `13:17:59`    justhodl-boj-full                            inv=7735     GB-s=219064   $3.65
- `13:17:59`    justhodl-repo                                inv=357      GB-s=185472   $3.09
- `13:17:59`    justhodl-sdmx-walker                         inv=13068    GB-s=115725   $1.93
- `13:17:59`    justhodl-provider-catalog                    inv=173      GB-s=67216    $1.12
- `13:17:59`    justhodl-ecb-deep                            inv=203      GB-s=65566    $1.09
- `13:17:59`    justhodl-news-velocity                       inv=168      GB-s=54355    $0.91
- `13:17:59`    justhodl-research-backtest                   inv=29       GB-s=49882    $0.83
- `13:17:59`    justhodl-katlin                              inv=23       GB-s=36858    $0.61
- `13:17:59`    justhodl-worldbank-full                      inv=137      GB-s=36368    $0.61
- `13:17:59`    justhodl-13f-positions                       inv=84       GB-s=26392    $0.44
- `13:17:59`    justhodl-repo-monitor                        inv=345      GB-s=16905    $0.28
- `13:17:59`    justhodl-backend-agent                       inv=672      GB-s=14482    $0.24
- `13:17:59`    justhodl-risk-gate                           inv=182      GB-s=14297    $0.24
## E. S3 loop surfaces (S3 → Lambda → same bucket)

- `13:17:59` ⚠   S3 notification → openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/report.json]
- `13:17:59` ⚠   S3 notification → openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/macro-nowcast.json]
- `13:17:59` ⚠   S3 notification → openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/compound-signals.json]
- `13:17:59` ⚠   S3 notification → openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/cross-asset-regime.json]
- `13:17:59` ⚠   S3 notification → openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/options-flow.json]
- `13:17:59` ⚠   S3 notification → openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/eurodollar-stress.json]
- `13:17:59` ⚠   S3 notification → openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/nobrainers.json]
- `13:17:59` ⚠   S3 notification → openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/narrative-density.json]
- `13:17:59`   EventBridge notifications on bucket: ENABLED
- `13:18:00` ⚠   aws.s3 rule justhodl-crypto-fanin → justhodl-crypto-intel
## F. GUARDRAILS — notifier, SNS wiring, alarms

- `13:18:00`   Lambda missing — creating
- `13:18:05` ✅   ✓ created justhodl-guardrail-notify
- `13:18:06` ✅   justhodl-guardrail-notify arn:aws:lambda:us-east-1:857687956942:function:justhodl-guardrail-notify
- `13:18:06`   topic exists: arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts
- `13:18:06` ✅   subscribed justhodl-guardrail-notify to the topic (Telegram + S3 ledger)
- `13:18:07` ⚠   email subscription requested for raafouis@gmail.com — PENDING until the confirmation link is clicked
- `13:18:07` ✅   alarm justhodl-guard-lambda-recursion-dropped    state=INSUFFICIENT_DATA
- `13:18:07` ✅   alarm justhodl-guard-lambda-recursion-dropped-account state=INSUFFICIENT_DATA
- `13:18:07` ✅   alarm justhodl-guard-lambda-invocations-1h       state=INSUFFICIENT_DATA
- `13:18:08` ✅   alarm justhodl-guard-lambda-errors-1h            state=INSUFFICIENT_DATA
- `13:18:08` ✅   alarm justhodl-guard-lambda-concurrency          state=INSUFFICIENT_DATA
- `13:18:08` ✅   alarm justhodl-guard-s3-bytes-growth-1d          state=INSUFFICIENT_DATA
- `13:18:09` ✅   alarm justhodl-guard-s3-objects-growth-1d        state=INSUFFICIENT_DATA
- `13:18:11` ⚠   delivery test: {"version": "1.0.0", "sent": 0, "failed": 0, "telegram_ok": false, "telegram_info": "HTTP Error 401: Unauthorized"}
## VERDICT

- `13:18:11` tripped functions (14d): justhodl-worldbank-full=6
- `13:18:11` walk engines that break at AWS's 16: 13 (section B, risk=BREAKS@16) — fix wave = ops 5251
- `13:18:11` alarms armed: 7/7; notifier: arn:aws:lambda:us-east-1:857687956942:function:justhodl-guardrail-notify
- `13:18:11` ✅ wrote 5250_lambda_recursion_forensics_guardrails.json
