
# 1) Build deployment zip

- `19:48:51`     zip size: 15,130b

# 2) Create or update Lambda

- `19:48:51`     creating new
- `19:48:55`     ✓ ready, state=Active

# 3) Schedule EventBridge daily 09:00 UTC

- `19:48:56`     ✓ permission added
- `19:48:56`     rule: justhodl-deep-value-screener-daily  expr=cron(0 9 * * ? *)

# 4) Smoke invoke

- `19:49:04`     status: 200  duration: 8.2s
- `19:49:04`     body: {"statusCode": 200, "body": "{\"n_universe\": 500, \"n_qualifying\": 59, \"n_tier_a\": 34, \"duration_s\": 7.3}"}
- `19:49:04`     ── tail ──
- `19:49:04`       START RequestId: 5b422391-d451-4337-b7da-589de4359fe7 Version: $LATEST
- `19:49:04`       [deep-value] starting v1.0, max_tickers=500, budget=240s
- `19:49:04`       [deep-value] seeded 503 tickers from screener/data.json
- `19:49:04`       [deep-value] universe size: 500
- `19:49:04`       [deep-value] evaluated 500, OK: 59, statuses: {'ok': 59, 'no_quote': 0, 'below_min_mcap': 0, 'no_balance': 0, 'below_min_net_cash': 441, 'no_income': 0, 'deadline_skip': 0}
- `19:49:04`       [deep-value] wrote 46650b to data/deep-value.json
- `19:49:04`       [deep-value] tier_a=34 tier_b=8 watch=16 contrarian=3
- `19:49:04`       [deep-value] TOP: [('EG', 100, 'DEEP_VALUE_TIER_A'), ('CNC', 100, 'DEEP_VALUE_TIER_A'), ('AIZ', 100, 'DEEP_VALUE_TIER_A'), ('PRU', 100, 'DEEP_VALUE_TIER_A'), ('MET', 100, 'DEEP_VALUE_TIER_A'), ('ALL', 100, 'DEEP_VALUE_TIER_A'), ('L', 95.7, 'DEEP_VALUE_TIER_A'), ('SYF', 94.2, 'DEEP_VALUE_TIER_A')]
- `19:49:04`       END RequestId: 5b422391-d451-4337-b7da-589de4359fe7
- `19:49:04`       REPORT RequestId: 5b422391-d451-4337-b7da-589de4359fe7	Duration: 7317.94 ms	Billed Duration: 7985 ms	Memory Size: 1024 MB	Max Memory Used: 106 MB	Init Duration: 666.29 ms

# 5) S3 output

- `19:49:04`     size: 46,650b
- `19:49:04`     schema: 1
- `19:49:04`     generated_at: 2026-05-05T19:49:04+00:00
- `19:49:04`     stats: {"n_universe": 500, "n_qualifying": 59, "n_tier_a": 34, "n_tier_b": 8, "n_watch": 16, "n_contrarian": 3, "statuses": {"ok": 59, "no_quote": 0, "below_min_mcap": 0, "no_balance": 0, "below_min_net_cash": 441, "no_income": 0, "deadline_skip": 0}}
- `19:49:04`   
- `19:49:04`     ── Top 15 deep-value setups ──
- `19:49:04`      # Symbol    Score Flag                      %NC   %Rev   M/R   %52H Sector                
- `19:49:04`      1 EG        100.0 DEEP_VALUE_TIER_A        312%   124%  0.81    -5%                       
- `19:49:04`      2 CNC       100.0 DEEP_VALUE_TIER_A         91%   740%  0.14   -17%                       
- `19:49:04`      3 AIZ       100.0 DEEP_VALUE_TIER_A         82%   108%  0.92    -4%                       
- `19:49:04`      4 PRU       100.0 DEEP_VALUE_TIER_A       1118%   175%  0.57   -17%                       
- `19:49:04`      5 MET       100.0 DEEP_VALUE_TIER_A        895%   147%  0.68    -4%                       
- `19:49:04`      6 ALL       100.0 DEEP_VALUE_TIER_A        139%   118%  0.85    -1%                       
- `19:49:04`      7 L          95.7 DEEP_VALUE_TIER_A        204%    83%  1.21    -7%                       
- `19:49:04`      8 SYF        94.2 DEEP_VALUE_TIER_A        389%    77%  1.30   -17%                       
- `19:49:04`      9 TRV        94.0 DEEP_VALUE_TIER_A        147%    76%  1.32    -3%                       
- `19:49:04`     10 PFG        92.9 DEEP_VALUE_TIER_A        508%    72%  1.40    -1%                       
- `19:49:04`     11 AIG        90.9 DEEP_VALUE_TIER_A        217%    64%  1.57   -10%                       
- `19:49:04`     12 ACGL       89.8 DEEP_VALUE_TIER_A        138%    59%  1.69    -9%                       
- `19:49:04`     13 COF        89.7 DEEP_VALUE_TIER_A        462%    59%  1.70   -27%                       
- `19:49:04`     14 PGR        89.5 DEEP_VALUE_TIER_A         74%    76%  1.32   -32%                       
- `19:49:04`     15 WRB        89.1 DEEP_VALUE_TIER_A        118%    56%  1.77   -15%                       