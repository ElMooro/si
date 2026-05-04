# Create justhodl-macro-surprise + smoke test

**Status:** success  
**Duration:** 11.6s  
**Finished:** 2026-05-04T12:15:30+00:00  

## Log
- `12:15:19`   zip size: 4,288b
- `12:15:19` ✅   ✓ created
## EventBridge schedule (6h)

- `12:15:20` ✅   ✓ wired
## Smoke test

- `12:15:30`   status: 200 duration: 2.0s
- `12:15:30`   resp: {"statusCode": 200, "body": "{\"composite_z\": 1.41, \"regime\": \"GROWTH_SURPRISE_POSITIVE\", \"n_indicators\": 6}"}
## S3 verify

- `12:15:30`   composite_z: 1.41
- `12:15:30`   growth_z: 0.66
- `12:15:30`   inflation_z: -2.49
- `12:15:30`   regime: GROWTH_SURPRISE_POSITIVE
- `12:15:30`   desc: Data beating expectations — bullish for risk assets
- `12:15:30`   n_indicators: 6/23
## 📊 By category

- `12:15:30`     CONSUMER       avg_z=-0.06 n_beat=0 n_miss=0 dir=INLINE
- `12:15:30`     HOUSING        avg_z=+0.14 n_beat=1 n_miss=1 dir=INLINE
- `12:15:30`     EMPLOYMENT     avg_z=+1.91 n_beat=1 n_miss=0 dir=BEATING
- `12:15:30`     INFLATION      avg_z=-2.49 n_beat=0 n_miss=1 dir=MISSING
## 🟢 Top BEATS (data above trend)

- `12:15:30`     HOUST          Housing Starts                         z=+2.08 dir=BEAT
- `12:15:30`     ICSA           Initial Jobless Claims                 z=+1.91 dir=BEAT
- `12:15:30`     UMCSENT        U Michigan Consumer Sentiment          z=+0.32 dir=INLINE
- `12:15:30`     PI             Personal Income                        z=-0.44 dir=INLINE
- `12:15:30`     EXHOSLUSM495S  Existing Home Sales                    z= -1.8 dir=MISS
## 🔴 Top MISSES (data below trend)

- `12:15:30`     PCEPILFE       Core PCE                               z=-2.49 dir=MISS
- `12:15:30`     EXHOSLUSM495S  Existing Home Sales                    z= -1.8 dir=MISS
- `12:15:30`     PI             Personal Income                        z=-0.44 dir=INLINE
- `12:15:30`     UMCSENT        U Michigan Consumer Sentiment          z=+0.32 dir=INLINE
- `12:15:30`     ICSA           Initial Jobless Claims                 z=+1.91 dir=BEAT
