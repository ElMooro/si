
# 1) Force-redeploy deep-value Lambda from current source

- `20:00:33`     source size: 15766 chars
- `20:00:35`     ✓ deployed, mod=2026-05-05T20:00:34.000+0000

# 2) Re-invoke deep-value

- `20:00:41`     status: 200
- `20:00:41`       START RequestId: 51f90ca8-d998-405f-914e-83e51b3b091f Version: $LATEST
- `20:00:41`       [deep-value] starting v1.0, max_tickers=500, budget=240s
- `20:00:41`       [deep-value] seeded 503 tickers from screener/data.json
- `20:00:41`       [deep-value] universe size: 500
- `20:00:41`       [deep-value] evaluated 500, OK: 35, statuses: {'ok': 35, 'no_quote': 158, 'below_min_mcap': 0, 'no_balance': 4, 'below_min_net_cash': 302, 'no_income': 1, 'deadline_skip': 0}
- `20:00:41`       [deep-value] wrote 29821b to data/deep-value.json
- `20:00:41`       [deep-value] tier_a=18 tier_b=3 watch=14 contrarian=1
- `20:00:41`       [deep-value] TOP: [('EG', 100, 'DEEP_VALUE_TIER_A'), ('CNC', 100, 'DEEP_VALUE_TIER_A'), ('AIZ', 100, 'DEEP_VALUE_TIER_A'), ('PRU', 100, 'DEEP_VALUE_TIER_A'), ('MET', 100, 'DEEP_VALUE_TIER_A'), ('SYF', 94.2, 'DEEP_VALUE_TIER_A'), ('TRV', 94.0, 'DEEP_VALUE_TIER_A'), ('PFG', 92.9, 'DEEP_VALUE_TIER_A')]
- `20:00:41`       END RequestId: 51f90ca8-d998-405f-914e-83e51b3b091f
- `20:00:41`       REPORT RequestId: 51f90ca8-d998-405f-914e-83e51b3b091f	Duration: 5319.73 ms	Billed Duration: 5780 ms	Memory Size: 1024 MB	Max Memory Used: 103 MB	Init Duration: 459.43 ms

# 3) Load all 5 signal feeds

- `20:00:42`     nobrainers: 25 entries from data/nobrainers.json
- `20:00:42`     insiders: 22 entries from data/insider-clusters.json
- `20:00:42`     smart_money: 85 entries from data/smart-money-clusters.json
- `20:00:42`     deep_value: 25 entries from data/deep-value.json
- `20:00:43`     eps_velocity: 25 entries from data/eps-revision-velocity.json

# 4) Compound-signal table — names on 2+ lists

- `20:00:43`     total names tracked: 96
- `20:00:43`     names on 2+ lists: 1
- `20:00:43`     names on 3+ lists: 0
- `20:00:43`   
- `20:00:43`     ── Compound leaderboard ──
- `20:00:43`     Sym    #Sys Systems hit                            Compound
- `20:00:43`     CSGP      2  eps_v, insid                                         220.7

# 5) Write data/compound-signals.json

- `20:00:43`     wrote 634b to data/compound-signals.json