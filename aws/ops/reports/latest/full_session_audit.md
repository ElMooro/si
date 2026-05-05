
# 0) Audit scope

- `21:39:33`     This audit checks every Lambda, S3 feed, and page from today's session.
- `21:39:33`     Issues are logged and listed at the end with severity tags.

# 1) NOBRAINER CHAIN (L1-L6) Lambdas

- `21:39:34`     ✓ justhodl-theme-detector                   cron(0 6 * * ? *)       code:✓  iss=-
- `21:39:35`     ✓ justhodl-supply-inflection-scanner        cron(0 7 * * ? *)       code:✓  iss=-
- `21:39:35`     ✓ justhodl-theme-tier-classifier            cron(0 8 * * ? *)       code:✓  iss=-
- `21:39:36`     ✓ justhodl-asymmetric-hunter                cron(30 13 * * ? *)     code:✓  iss=-
- `21:39:36`     ✓ justhodl-nobrainer-rationale              cron(45 13 * * ? *)     code:✓  iss=-
- `21:39:37`     ✓ justhodl-nobrainer-tracker                rate(1 hour)            code:✓  iss=-

# 2) NEW HUNTER Lambdas

- `21:39:37`     ✓ justhodl-insider-cluster-scanner          cron(30 14 * * ? *)     code:✓  iss=-
- `21:39:38`     ✓ justhodl-smart-money-cluster              cron(0 9 * * ? *)       code:✓  iss=-
- `21:39:38`     ✓ justhodl-deep-value-screener              cron(0 9 * * ? *)       code:✓  iss=-
- `21:39:39`     ✓ justhodl-eps-revision-velocity            cron(30 9 * * ? *)      code:✓  iss=-

# 3) INFRASTRUCTURE Lambdas (backtest, brief, monitor, calibration)

- `21:39:39`     ✓ justhodl-backtest-engine                  rate(6 hours)           state=Active  iss=-
- `21:39:39`     ✓ justhodl-ai-brief                         cron(5 0,4,8,12,16,20 ? * * *)  state=Active  iss=-
- `21:39:39`     ✓ justhodl-position-monitor                 rate(30 minutes)        state=Active  iss=-
- `21:39:40`     ✓ justhodl-signal-logger                    rate(6 hours)           state=Active  iss=-
- `21:39:40`     ✓ justhodl-outcome-checker                  cron(30 22 ? * MON-FRI *)  state=Active  iss=-
- `21:39:40`     ✓ justhodl-calibrator                       cron(0 9 ? * SUN *)     state=Active  iss=-
- `21:39:40`     ✓ justhodl-cot-extremes-scanner             cron(0 19 ? * FRI *)    state=Active  iss=-
- `21:39:41`     ✓ justhodl-asymmetric-scorer                cron(30 13 ? * MON-FRI *)  state=Active  iss=-
- `21:39:41`     ✓ justhodl-risk-sizer                       cron(45 13 ? * MON-FRI *)  state=Active  iss=-
- `21:39:41`     ✓ justhodl-auction-crisis-detector          cron(0/15 14-22 ? * MON-FRI *)  state=Active  iss=-
- `21:39:41`     ✓ justhodl-eurodollar-stress                rate(1 hour)            state=Active  iss=-

# 4) S3 DATA FEEDS — all 6 system outputs + supporting

- `21:39:42`     ✓ data/themes-detected.json                     57,869b    471min — L1 themes
- `21:39:42`     ✓ data/supply-inflection.json                   68,210b    439min — L2 supply
- `21:39:42`     ✓ data/theme-tiers.json                        306,322b    424min — L3 tiers
- `21:39:42`     ✓ data/nobrainers.json                         456,897b    307min — L4 hunter
- `21:39:42`     ✓ data/nobrainers-rationale.json                53,198b    115min — L5 rationale
- `21:39:43`     ✓ data/insider-clusters.json                    43,345b    170min — Insider scanner
- `21:39:43`     ✓ data/smart-money-clusters.json               152,894b    130min — 13F smart money
- `21:39:43`     ✓ data/deep-value.json                          46,659b      9min — Deep value
- `21:39:43`     ✓ data/eps-revision-velocity.json               72,283b    104min — EPS velocity
- `21:39:43`     ✓ data/compound-signals.json                     2,656b      9min — Compound aggregator
- `21:39:44`     ✓ data/13f-positions.json                   14,607,267b    288min — Raw 13F input
- `21:39:44`     ✓ data/decisive-call-history.json                4,989b     93min — Calls ledger
- `21:39:44`     ✓ backtest/results.json                         21,068b    272min — Backtest results
- `21:39:44`     ✓ backtest/summary.json                          3,473b    272min — Backtest summary
- `21:39:44`     ❌ portfolio/positions.json                           0b    999min — Paper positions
- `21:39:44`     ✓ data/report.json                           1,754,249b      4min — Daily liquidity report
- `21:39:45`     ✓ data/auction-crisis.json                      11,431b      9min — Auction crisis

# 5) PAGES — HTTP 200 check + nav presence

- `21:39:45`     ✓ 200    64,898b  https://justhodl.ai/  iss=-
- `21:39:45`     ✓ 200    18,081b  https://justhodl.ai/compound-signals.html  iss=-
- `21:39:45`     ✓ 200    20,795b  https://justhodl.ai/nobrainers.html  iss=-
- `21:39:45`     ✓ 200    19,378b  https://justhodl.ai/insider-clusters.html  iss=-
- `21:39:45`     ✓ 200    21,137b  https://justhodl.ai/smart-money.html  iss=-
- `21:39:45`     ✓ 200    12,333b  https://justhodl.ai/deep-value.html  iss=-
- `21:39:46`     ✓ 200    12,147b  https://justhodl.ai/eps-velocity.html  iss=-
- `21:39:46`     ✓ 200    15,640b  https://justhodl.ai/themes.html  iss=-
- `21:39:46`     ✓ 200    19,472b  https://justhodl.ai/brief.html  iss=-
- `21:39:46`     ✓ 200    23,882b  https://justhodl.ai/calls.html  iss=-
- `21:39:46`     ✓ 200    27,696b  https://justhodl.ai/desk.html  iss=-
- `21:39:46`     ✓ 200    46,842b  https://justhodl.ai/backtest.html  iss=-
- `21:39:46`     ✓ 200    17,540b  https://justhodl.ai/horizons.html  iss=-
- `21:39:46`     ✓ 200    19,064b  https://justhodl.ai/sizing.html  iss=-
- `21:39:46`     ✓ 200    24,899b  https://justhodl.ai/weights.html  iss=-
- `21:39:47`     ✓ 200    22,356b  https://justhodl.ai/performance.html  iss=-
- `21:39:47`     ✓ 200    29,413b  https://justhodl.ai/13f.html  iss=-

# 6) DATA QUALITY DEEP CHECKS

- `21:39:47`     ── compound-signals.json ──
- `21:39:47`       feed_stats: {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 5, "eps_velocity": 25}
- `21:39:47`       total_names: 156
- `21:39:47`       multi-signal: 6
- `21:39:47`       3+ systems: 0
- `21:39:47`       CSGP   #2  systems=['eps_velocity', 'insiders']  comp=220.7
- `21:39:47`       OXY    #2  systems=['nobrainers', 'smart_money']  comp=178.4
- `21:39:47`       HUM    #2  systems=['deep_value', 'smart_money']  comp=177.5
- `21:39:47`       WFC    #2  systems=['deep_value', 'smart_money']  comp=172.3
- `21:39:47`       BAC    #2  systems=['deep_value', 'smart_money']  comp=165.3
- `21:39:47`   
- `21:39:47`     ── deep-value.json sector accuracy ──
- `21:39:47`       top_25_overall: 5
- `21:39:47`       top_25_excluded: 25
- `21:39:47`       ⚠ 2 entries with blank sector (may indicate FMP /profile failures)
- `21:39:47`         BAC     flag=DEEP_VALUE_TIER_A
- `21:39:47`         WFC     flag=DEEP_VALUE_TIER_B
- `21:39:47`   
- `21:39:47`     ── insider-clusters.json freshness ──
- `21:39:47`       n_clusters: 22
- `21:39:47`       n_strong: 8
- `21:39:47`       smart_money_dual: 4
- `21:39:47`   
- `21:39:47`     ── smart-money-clusters.json ──
- `21:39:47`       total: 85
- `21:39:47`       MOH     score=  86.0  signals=['NEW_INITIATION_CLUSTER', 'DEEP_VALUE_CONSENSUS', 'LEGEND_FUND_BUY']
- `21:39:47`       LLY     score=  82.8  signals=['NEW_INITIATION_CLUSTER', 'CONSENSUS_BUY', 'LEGEND_FUND_BUY']
- `21:39:47`       AMZN    score=  76.4  signals=['NEW_INITIATION_CLUSTER', 'LEGEND_FUND_BUY']
- `21:39:47`       CAH     score=  75.1  signals=['NEW_INITIATION_CLUSTER', 'CONSENSUS_BUY', 'LEGEND_FUND_BUY']
- `21:39:47`       AXP     score=  74.1  signals=['CONSENSUS_BUY', 'LEGEND_FUND_BUY']

# 7) ISSUE SUMMARY

- `21:39:47`     ✓ No critical issues found
- `21:39:47`   
- `21:39:47`     Audit took 13.6s