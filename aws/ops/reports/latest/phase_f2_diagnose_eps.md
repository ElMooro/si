
# 1) Check what's in EPS universe today

- `22:46:23`     has 'universe.json': True
- `22:46:23`     has 'PRIMARY: unified': True

# 2) Read universe.json — what tickers are in there

- `22:46:23`     universe has 336 tickers
- `22:46:23`       AAPL in universe: True
- `22:46:23`       MSFT in universe: False
- `22:46:23`       NVDA in universe: False
- `22:46:23`       GOOGL in universe: True
- `22:46:23`       AMZN in universe: True
- `22:46:23`       META in universe: False
- `22:46:23`       TSLA in universe: False
- `22:46:23`       MU in universe: False
- `22:46:23`       SNDK in universe: False
- `22:46:23`       PLTR in universe: False
- `22:46:23`       CSGP in universe: True
- `22:46:23`       EPAM in universe: True

# 3) Probe FMP /stable/analyst-estimates directly

- `22:46:23`       AAPL: 5 estimates returned
- `22:46:23`          sample keys: ['symbol', 'date', 'revenueLow', 'revenueHigh', 'revenueAvg', 'ebitdaLow', 'ebitdaHigh', 'ebitdaAvg']
- `22:46:23`          sample date: 2030-09-27, eps_avg: 12.5
- `22:46:23`       MSFT: 5 estimates returned
- `22:46:23`          sample keys: ['symbol', 'date', 'revenueLow', 'revenueHigh', 'revenueAvg', 'ebitdaLow', 'ebitdaHigh', 'ebitdaAvg']
- `22:46:23`          sample date: 2030-06-30, eps_avg: 33.345
- `22:46:23`       NVDA: 5 estimates returned
- `22:46:23`          sample keys: ['symbol', 'date', 'revenueLow', 'revenueHigh', 'revenueAvg', 'ebitdaLow', 'ebitdaHigh', 'ebitdaAvg']
- `22:46:23`          sample date: 2031-01-25, eps_avg: 13.56
- `22:46:24`       PLTR: 5 estimates returned
- `22:46:24`          sample keys: ['symbol', 'date', 'revenueLow', 'revenueHigh', 'revenueAvg', 'ebitdaLow', 'ebitdaHigh', 'ebitdaAvg']
- `22:46:24`          sample date: 2030-12-31, eps_avg: 8.94
- `22:46:24`       CSGP: 5 estimates returned
- `22:46:24`          sample keys: ['symbol', 'date', 'revenueLow', 'revenueHigh', 'revenueAvg', 'ebitdaLow', 'ebitdaHigh', 'ebitdaAvg']
- `22:46:24`          sample date: 2030-12-31, eps_avg: 3.67856

# 4) Re-invoke EPS with verbose log capture

- `22:46:29`     status: 200, dur: 5.6s
- `22:46:29`     body: {'statusCode': 200, 'body': '{"n_universe": 400, "n_qualifying": 218, "n_tier_a": 0, "n_tier_b": 53, "duration_s": 5.6}'}
- `22:46:29`     ── full tail ──
- `22:46:29`       START RequestId: 9bd85aaa-cd49-4a01-9d3c-f8edafff0bf4 Version: $LATEST
- `22:46:29`       [eps-velocity] starting v1.0, max_tickers=400
- `22:46:29`       [eps-velocity] seeded 336 from data/universe.json (unified)
- `22:46:29`       [eps-velocity] universe after screener fallback: 573
- `22:46:29`       [eps-velocity] universe size: 400
- `22:46:29`       [eps-velocity] OK: 218, statuses: {'ok': 218, 'below_min_velocity': 31}
- `22:46:29`       [eps-velocity] wrote 144390b to data/eps-revision-velocity.json
- `22:46:29`       [eps-velocity] tier_a=0 tier_b=53
- `22:46:29`       [eps-velocity] TOP: [('AMD', 86.0, 'HIGH_VELOCITY_TIER_B'), ('AVGO', 85.0, 'HIGH_VELOCITY_TIER_B'), ('BE', 85.0, 'HIGH_VELOCITY_TIER_B'), ('APP', 81.5, 'HIGH_VELOCITY_TIER_B'), ('AXON', 81.2, 'HIGH_VELOCITY_TIER_B'), ('COHR', 80.2, 'HIGH_VELOCITY_TIER_B'), ('FCX', 79.3, 'HIGH_VELOCITY_TIER_B'), ('KLAC', 78.8, 'HIGH_VELOCITY_TIER_B')]
- `22:46:29`       END RequestId: 9bd85aaa-cd49-4a01-9d3c-f8edafff0bf4
- `22:46:29`       REPORT RequestId: 9bd85aaa-cd49-4a01-9d3c-f8edafff0bf4	Duration: 5593.78 ms	Billed Duration: 5594 ms	Memory Size: 1024 MB	Max Memory Used: 110 MB