
# 1) Verify deep-value deployed code matches repo

- `21:20:06`     repo source has top_25_excluded_financials: True
- `21:20:06`     deployed code has top_25_excluded_financials: True
- `21:20:06`     ✓ deployed code matches repo

# 2) Re-invoke deep-value

- `21:20:13`     status: 200
- `21:20:13`     body: {"n_universe": 500, "n_qualifying": 34, "n_tier_a": 16, "duration_s": 5.3}
- `21:20:13`     ── top 8 in deep-value top_25 ──
- `21:20:13`       EG      100.0  DEEP_VALUE_TIER_A     
- `21:20:13`       CNC     100.0  DEEP_VALUE_TIER_A     
- `21:20:13`       AIZ     100.0  DEEP_VALUE_TIER_A     
- `21:20:13`       MET     100.0  DEEP_VALUE_TIER_A     
- `21:20:13`       SYF      94.2  DEEP_VALUE_TIER_A     
- `21:20:13`       TRV      94.0  DEEP_VALUE_TIER_A     
- `21:20:13`       PFG      92.9  DEEP_VALUE_TIER_A     
- `21:20:13`       ACGL     89.9  DEEP_VALUE_TIER_A     
- `21:20:13`     ── top 5 excluded (financials/REITs) ──

# 3) Inspect smart-money cluster schema

- `21:20:13`     total clusters: 85
- `21:20:13`     sample fields: ['ticker', 'name', 'score', 'flag', 'signal_types', 'n_funds_holding', 'n_buyers', 'n_sellers', 'n_new', 'legend_buyers', 'quant_buyers', 'total_value', 'pct_from_52w_high', 'components', 'rationale', 'fund_actions', 'fundamentals']
- `21:20:13`       possible ticker field: ticker = 'MOH'
- `21:20:13`   
- `21:20:13`     ── top 8 by score ──
- `21:20:13`       score=  86.0  ticker='MOH'       symbol=''          signal=
- `21:20:13`       score=  82.8  ticker='LLY'       symbol=''          signal=
- `21:20:13`       score=  76.4  ticker='AMZN'      symbol=''          signal=
- `21:20:13`       score=  75.1  ticker='CAH'       symbol=''          signal=
- `21:20:13`       score=  74.1  ticker='AXP'       symbol=''          signal=
- `21:20:13`       score=  72.0  ticker='AVGO'      symbol=''          signal=
- `21:20:13`       score=  67.0  ticker='ALLY'      symbol=''          signal=
- `21:20:13`       score=  66.3  ticker='CHKP'      symbol=''          signal=

# 4) Re-aggregate compound signals with correct schemas

- `21:20:13`     using smart-money ticker field: 'ticker'
- `21:20:14`     ✓ wrote 2430b
- `21:20:14`     feed_stats: {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 25, "eps_velocity": 25}
- `21:20:14`     total tracked: 176, multi: 6, 3+: 0
- `21:20:14`   
- `21:20:14`     ── full compound leaderboard ──
- `21:20:14`     CSGP   #2  score=  220.7  (eps_velocity,insiders)
- `21:20:14`     AMP    #2  score=  207.8  (deep_value,smart_money)
- `21:20:14`     OXY    #2  score=  178.4  (nobrainers,smart_money)
- `21:20:14`     HUM    #2  score=  177.5  (deep_value,smart_money)
- `21:20:14`     GS     #2  score=  157.5  (deep_value,smart_money)
- `21:20:14`     FCX    #2  score=  156.9  (nobrainers,smart_money)