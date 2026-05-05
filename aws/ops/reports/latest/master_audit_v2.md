
# A) Hunter Lambdas — deployed code matches repo + schedule

- `22:19:47`     ✓ L1          justhodl-theme-detector                   mem=1024MB  to=300s  sched=cron(0 6 * * ? *)       code:✓  mod=2026-05-05T13:40
- `22:19:48`     ✓ L2          justhodl-supply-inflection-scanner        mem=1024MB  to=300s  sched=cron(0 7 * * ? *)       code:✓  mod=2026-05-05T14:20
- `22:19:48`     ✓ L3          justhodl-theme-tier-classifier            mem=1024MB  to=600s  sched=cron(0 8 * * ? *)       code:✓  mod=2026-05-05T14:35
- `22:19:49`     ✓ L4          justhodl-asymmetric-hunter                mem=1024MB  to=600s  sched=cron(30 13 * * ? *)     code:✓  mod=2026-05-05T14:44
- `22:19:49`     ✓ L5          justhodl-nobrainer-rationale              mem= 512MB  to=600s  sched=cron(45 13 * * ? *)     code:✓  mod=2026-05-05T21:59
- `22:19:50`     ✓ L6          justhodl-nobrainer-tracker                mem= 512MB  to=300s  sched=rate(1 hour)            code:✓  mod=2026-05-05T16:52
- `22:19:50`     ✓ Insider     justhodl-insider-cluster-scanner          mem=1536MB  to=900s  sched=cron(30 14 * * ? *)     code:✓  mod=2026-05-05T18:39
- `22:19:50`     ✓ SmartMoney  justhodl-smart-money-cluster              mem= 512MB  to=300s  sched=cron(0 16 * * ? *)      code:✓  mod=2026-05-05T19:29
- `22:19:51`     ✓ DeepValue   justhodl-deep-value-screener              mem=1024MB  to=300s  sched=cron(0 9 * * ? *)       code:✓  mod=2026-05-05T21:54
- `22:19:51`     ✓ EPSVel      justhodl-eps-revision-velocity            mem=1024MB  to=300s  sched=cron(30 9 * * ? *)      code:✓  mod=2026-05-05T19:55
- `22:19:52`     ✓ Compound    justhodl-compound-aggregator              mem= 512MB  to=120s  sched=rate(1 hour)            code:✓  mod=2026-05-05T21:45
- `22:19:52`     ✓ SigLog      justhodl-system-signal-logger             mem= 512MB  to=300s  sched=rate(6 hours)           code:✓  mod=2026-05-05T22:05

# B) S3 data feeds — fresh, parseable, sensible

- `22:19:52`     ✓ data/themes-detected.json                      57,869b     511min — L1 themes  
- `22:19:52`     ✓ data/supply-inflection.json                    68,210b     479min — L2 supply  
- `22:19:52`     ✓ data/theme-tiers.json                         306,322b     464min — L3 tiers  
- `22:19:52`     ✓ data/nobrainers.json                          456,897b     347min — L4 hunter  
- `22:19:52`     ✓ data/nobrainers-rationale.json                 52,226b      18min — L5 rationale  
- `22:19:53`     ✓ data/insider-clusters.json                     43,345b     210min — Insider  
- `22:19:53`     ✓ data/smart-money-clusters.json                152,894b     171min — SmartMoney  
- `22:19:53`     ✓ data/deep-value.json                           33,739b      25min — DeepValue  
- `22:19:53`     ✓ data/eps-revision-velocity.json                72,283b     144min — EPSVelocity  
- `22:19:53`     ✓ data/compound-signals.json                      2,947b      25min — Compound  
- `22:19:53`     ✓ data/compound-signals-state.json                  140b      25min — CompoundState  
- `22:19:53`     ✓ data/13f-positions.json                     14,607,267b     329min — Raw13F  
- `22:19:53`     ✓ data/decisive-call-history.json                 4,989b     134min — Calls  
- `22:19:53`     ✓ backtest/results.json                          21,068b     312min — Backtest  
- `22:19:53`     ✓ data/report.json                            1,754,426b       5min — DailyLiquidity  

# C) Live page check — HTTP 200 + nav links

- `22:19:53`     ✓ 200    59,518b  https://justhodl.ai/                                          -
- `22:19:53`     ⚠ 200    17,994b  https://justhodl.ai/compound-signals.html                     no_dv_nav,no_eps_nav
- `22:19:53`     ⚠ [LOW] https://justhodl.ai/compound-signals.html: missing nav: ['no_dv_nav', 'no_eps_nav']
- `22:19:53`     ⚠ 200    20,709b  https://justhodl.ai/nobrainers.html                           no_compound_nav,no_dv_nav,no_eps_nav
- `22:19:53`     ⚠ [LOW] https://justhodl.ai/nobrainers.html: missing nav: ['no_compound_nav', 'no_dv_nav', 'no_eps_nav']
- `22:19:53`     ✓ 200    19,332b  https://justhodl.ai/insider-clusters.html                     -
- `22:19:53`     ✓ 200    21,073b  https://justhodl.ai/smart-money.html                          -
- `22:19:53`     ✓ 200    12,298b  https://justhodl.ai/deep-value.html                           -
- `22:19:53`     ✓ 200    12,108b  https://justhodl.ai/eps-velocity.html                         -
- `22:19:53`     ✓ 200    15,582b  https://justhodl.ai/themes.html                               -
- `22:19:53`     ✓ 200    19,373b  https://justhodl.ai/brief.html                                -
- `22:19:54`     ✓ 200    23,792b  https://justhodl.ai/calls.html                                -
- `22:19:54`     ✓ 200    27,367b  https://justhodl.ai/desk.html                                 -
- `22:19:54`     ✓ 200    46,552b  https://justhodl.ai/backtest.html                             -
- `22:19:54`     ✓ 200    17,481b  https://justhodl.ai/horizons.html                             -
- `22:19:54`     ✓ 200    18,993b  https://justhodl.ai/sizing.html                               -

# D) Compound signals — quality check

- `22:19:54`     schema: 2
- `22:19:54`     feed_stats: {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 9, "eps_velocity": 25}
- `22:19:54`     stats: {"n_total_names": 161, "n_multi_signal": 5, "n_3_plus": 0, "n_compound_over_200": 2, "n_compound_over_300": 0}
- `22:19:54`     compound entries: 5
- `22:19:54`   
- `22:19:54`     ── full compound leaderboard ──
- `22:19:54`       CSGP    #sys=2  comp=  220.7  (eps_velocity, insiders)
- `22:19:54`       EPAM    #sys=2  comp=  213.0  (deep_value, insiders)
- `22:19:54`       OXY     #sys=2  comp=  178.4  (nobrainers, smart_money)
- `22:19:54`       HUM     #sys=2  comp=  177.5  (deep_value, smart_money)
- `22:19:54`       FCX     #sys=2  comp=  156.9  (nobrainers, smart_money)
- `22:19:54`   
- `22:19:54`     ── per-system top 3 ──
- `22:19:54`       nobrainers: ['TX', 'USAR', 'CSTM']
- `22:19:54`       insiders: ['SRAD', 'SPGI', 'SUNE']
- `22:19:54`       smart_money: ['MOH', 'LLY', 'AMZN']
- `22:19:54`       deep_value: ['CNC', 'HUM', 'EPAM']
- `22:19:54`       eps_velocity: ['PLTR', 'SNDK', 'LITE']

# E) DynamoDB justhodl-signals — per-source signal log

- `22:19:55`     signals logged in last 24h: 16
- `22:19:55`     by source: {'insider_cluster': 3, 'eps_velocity': 6, 'smart_money': 3, 'compound': 2, 'deep_value': 2}
- `22:19:55`     unique tickers: 13
- `22:19:55`     sample tickers: ['AXON', 'AXP', 'CHKP', 'DELL', 'EPAM', 'FCX', 'LYV', 'ON', 'PODD', 'PSUS', 'SRAD', 'SUNE', 'UBER']

# F) End-to-end smoke — trigger compound + verify pages render data

- `22:19:56`     compound-aggregator: status=200 n_compound=5 n_3plus=0 alerts=0
- `22:19:56`     compound page has fetch logic: True

# G) Issues found — prioritized

- `22:19:56`     HIGH (0):
- `22:19:56`     LOW (2):
- `22:19:56`       1. https://justhodl.ai/compound-signals.html: missing nav: ['no_dv_nav', 'no_eps_nav']
- `22:19:56`       2. https://justhodl.ai/nobrainers.html: missing nav: ['no_compound_nav', 'no_dv_nav', 'no_eps_nav']
- `22:19:56`   
- `22:19:56`     Audit took 9.0s