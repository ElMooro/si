
# 1) Inspect universe.json top-level schema

- `22:55:42`     size: 104,515b
- `22:55:42`     type: dict
- `22:55:42`     keys: ['duration_s', 'generated_at', 'schema_version', 'stats', 'stocks']
- `22:55:42`       duration_s: float = 9.1
- `22:55:42`       generated_at: str = 2026-05-05T22:37:54+00:00
- `22:55:42`       schema_version: int = 2
- `22:55:42`       stats: dict[5]
- `22:55:42`       stocks: list[336]
- `22:55:42`         first item keys: ['avg_volume', 'exchange', 'industry', 'market_cap', 'name', 'pct_from_52w_high', 'pct_from_52w_low', 'price', 'sector', 'symbol', 'volume', 'year_high', 'year_low']

# 2) Pull all tickers from universe with a robust reader

- `22:55:42`     found 336 via key 'stocks'

# 3) Spot check key names

- `22:55:42`     present: ['AAPL', 'GOOGL', 'AMZN', 'CSGP', 'EPAM', 'FCX', 'CNC', 'HUM', 'AVGO', 'AMD', 'JPM']
- `22:55:42`     total in universe: 336

# 4) Confirm FCX tier-3 compound finding

- `22:55:42`     ✓ FCX confirmed: n_systems=3, compound=367.8
- `22:55:42`       systems: ['eps_velocity', 'nobrainers', 'smart_money']
- `22:55:42`       nobrainers: {"theme": "PICK", "tier": 2, "flag": "TIER_B_HIGH_CONVICTION", "name": "Freeport-McMoRan Inc."}
- `22:55:42`       smart_money: {"signal_types": ["LEGEND_FUND_BUY"], "n_buyers": 2, "n_sellers": 7, "legend_buyers": ["LONE_PINE"], "name": "Freeport-McMoRan"}
- `22:55:42`       eps_velocity: {"flag": "HIGH_VELOCITY_TIER_B", "fy2_lift_pct": 47.2, "fwd_rev_growth_pct": 21.5, "company": "Freeport-McMoRan Inc."}