
# 1) Build zip

- `19:55:38`     zip: 15,850b

# 2) Create or update Lambda

- `19:55:38`     creating new
- `19:55:44`     ✓ ready

# 3) Schedule daily 09:30 UTC

- `19:55:45`     ✓ permission added

# 4) Smoke invoke

- `19:55:50`     status: 200  duration: 5.0s
- `19:55:50`     body: {"statusCode": 200, "body": "{\"n_universe\": 400, \"n_qualifying\": 104, \"n_tier_a\": 0, \"n_tier_b\": 44, \"duration_s\": 4.1}"}
- `19:55:50`     ── tail ──
- `19:55:50`       START RequestId: d1e6d59c-2cde-4be1-a117-dee07e592060 Version: $LATEST
- `19:55:50`       [eps-velocity] starting v1.0, max_tickers=400
- `19:55:50`       [eps-velocity] seeded 503 from screener
- `19:55:50`       [eps-velocity] universe size: 400
- `19:55:50`       [eps-velocity] OK: 104, statuses: {'ok': 104, 'below_min_velocity': 13}
- `19:55:50`       [eps-velocity] wrote 72283b to data/eps-revision-velocity.json
- `19:55:50`       [eps-velocity] tier_a=0 tier_b=44
- `19:55:50`       [eps-velocity] TOP: [('PLTR', 87.8, 'HIGH_VELOCITY_TIER_B'), ('SNDK', 86.0, 'HIGH_VELOCITY_TIER_B'), ('LITE', 85.0, 'HIGH_VELOCITY_TIER_B'), ('APP', 81.5, 'HIGH_VELOCITY_TIER_B'), ('AXON', 81.2, 'HIGH_VELOCITY_TIER_B'), ('COHR', 80.2, 'HIGH_VELOCITY_TIER_B'), ('VRT', 78.8, 'HIGH_VELOCITY_TIER_B'), ('CVNA', 78.5, 'HIGH_VELOCITY_TIER_B')]
- `19:55:50`       END RequestId: d1e6d59c-2cde-4be1-a117-dee07e592060
- `19:55:50`       REPORT RequestId: d1e6d59c-2cde-4be1-a117-dee07e592060	Duration: 4128.65 ms	Billed Duration: 4710 ms	Memory Size: 1024 MB	Max Memory Used: 108 MB	Init Duration: 580.41 ms

# 5) S3 output

- `19:55:50`     generated_at: 2026-05-05T19:55:50+00:00
- `19:55:50`     stats: {"n_universe": 400, "n_qualifying": 104, "n_tier_a": 0, "n_tier_b": 44, "statuses": {"ok": 104, "below_min_velocity": 13}}
- `19:55:50`   
- `19:55:50`     ── Top 15 EPS-velocity setups ──
- `19:55:50`      # Sym     Score Flag                    Lift%  RevG%   Up% NEst Sector                
- `19:55:50`      1 PLTR     87.8 HIGH_VELOCITY_TIER_B    +42.3  +43.2   19%   13                       
- `19:55:50`      2 SNDK     86.0 HIGH_VELOCITY_TIER_B   +166.4 +107.8    7%    9                       
- `19:55:50`      3 LITE     85.0 HIGH_VELOCITY_TIER_B   +116.1  +77.0    0%   12                       
- `19:55:50`      4 APP      81.5 HIGH_VELOCITY_TIER_B    +32.5  +29.8    0%   18                       
- `19:55:50`      5 AXON     81.2 HIGH_VELOCITY_TIER_B    +35.6  +29.4    0%   12                       
- `19:55:50`      6 COHR     80.2 HIGH_VELOCITY_TIER_B    +41.9  +27.7    0%   10                       
- `19:55:50`      7 VRT      78.8 HIGH_VELOCITY_TIER_B    +33.8  +25.8    0%   13                       
- `19:55:50`      8 CVNA     78.5 HIGH_VELOCITY_TIER_B    +36.4  +25.2    0%   11                       
- `19:55:50`      9 TER      77.8 HIGH_VELOCITY_TIER_B    +35.1  +22.1    9%   11                       
- `19:55:50`     10 CIEN     77.1 HIGH_VELOCITY_TIER_B    +39.4  +21.8    6%   11                       
- `19:55:50`     11 HOOD     76.8 HIGH_VELOCITY_TIER_B    +35.8  +22.7    0%   15                       
- `19:55:50`     12 COIN     75.9 HIGH_VELOCITY_TIER_B    +85.7  +21.3    0%   18                       
- `19:55:50`     13 SMCI     75.8 HIGH_VELOCITY_TIER_B    +34.1  +21.2    0%   10                       
- `19:55:50`     14 DASH     74.9 HIGH_VELOCITY_TIER_B    +75.9  +19.8    0%   29                       
- `19:55:50`     15 BX       74.4 HIGH_VELOCITY_TIER_B    +26.6  +25.6    7%   14                       