# Create justhodl-historical-analogs + smoke test

**Status:** success  
**Duration:** 31.4s  
**Finished:** 2026-05-04T12:25:59+00:00  

## Log
- `12:25:28`   zip size: 4,527b
- `12:25:28` ✅   ✓ created
## EventBridge schedule (daily 13 UTC)

- `12:25:29` ✅   ✓ wired
## Smoke test

- `12:25:59`   status: 200 duration: 21.5s
- `12:25:59`   resp: {"statusCode": 200, "body": "{\"n_analogs\": 15, \"directional_call\": \"BULLISH\", \"duration_s\": 20.62}"}
## S3 verify

- `12:25:59`   today_date: 2026-04-24
- `12:25:59`   vix: 18.71
- `12:25:59`   2s10s: 0.53 bps
- `12:25:59`   hy_oas: 2.86%
- `12:25:59`   usd_index: 118.7294
- `12:25:59`   10Y yield: 4.31%
- `12:25:59`   spx_1m_return: 8.7%
- `12:25:59`   call: BULLISH
- `12:25:59`   desc: 21d analogs: 100.0% positive, mean +2.54%
- `12:25:59`   n_historical_evaluated: 650
## 📊 Forward return distribution

- `12:25:59`     5d     n= 15 mean=+0.09% median=+0.45% hit_rate=60.0% range=[-2.4, +1.8]%
- `12:25:59`     21d    n= 15 mean=+2.54% median=+1.96% hit_rate=100.0% range=[+0.2, +5.0]%
- `12:25:59`     63d    n= 15 mean=+8.02% median=+8.25% hit_rate=100.0% range=[+5.5, +10.0]%
- `12:25:59`     126d   n= 15 mean=+12.43% median=+13.28% hit_rate=100.0% range=[+7.8, +16.3]%
## 🔍 Top 8 analogs

- `12:25:59`     2025-06-10 dist=1.896 sim=0.62 21d=+3.66% 63d=+8.17%
- `12:25:59`     2025-06-06 dist=1.949 sim=0.61 21d=+4.38% 63d=+8.25%
- `12:25:59`     2025-05-13 dist=1.984 sim=0.60 21d=+2.70% 63d=+9.85%
- `12:25:59`     2025-05-14 dist=1.995 sim=0.60 21d=+1.43% 63d=+9.77%
- `12:25:59`     2025-06-09 dist=2.025 sim=0.59 21d=+4.57% 63d=+8.44%
- `12:25:59`     2025-06-05 dist=2.031 sim=0.59 21d=+4.82% 63d=+9.13%
- `12:25:59`     2025-07-22 dist=2.040 sim=0.59 21d=+1.37% 63d=+6.74%
- `12:25:59`     2025-07-03 dist=2.067 sim=0.59 21d=+0.81% 63d=+6.94%
