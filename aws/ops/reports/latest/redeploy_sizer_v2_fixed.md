# 1) Wait + redeploy

**Status:** success  
**Duration:** 12.5s  
**Finished:** 2026-05-05T12:17:24+00:00  

## Log
- `12:17:14` ✅   ✓ deployed, mod=2026-05-05T12:17:12.000+0000
# 2) Invoke

- `12:17:15`   status: 200, duration: 1.4s
- `12:17:15`   current_exposure_pct: 2.7231
- `12:17:15`   recommended_exposure_pct: 0.22
- `12:17:15`   actions: {'TRIM': 11}
# 3) Inspect positions with proper sizes

- `12:17:15`   All 11 positions with current vs recommended:
- `12:17:15`     ABBV      src=short_squeeze       hor=day_30  w=0.700 cur= 25.00% ($24,999) → rec= 2.00%  Δ=-23.00pp  [TRIM]
- `12:17:15`     NOW       src=earnings_pead       hor=day_30  w=0.700 cur= 24.98% ($24,978) → rec= 2.00%  Δ=-22.98pp  [TRIM]
- `12:17:15`     ELV       src=earnings_pead       hor=day_30  w=0.700 cur= 24.97% ($24,970) → rec= 2.00%  Δ=-22.97pp  [TRIM]
- `12:17:15`     QCOM      src=earnings_pead       hor=day_30  w=0.700 cur= 24.96% ($24,958) → rec= 2.00%  Δ=-22.96pp  [TRIM]
- `12:17:15`     ROKU      src=earnings_pead       hor=day_30  w=0.700 cur= 24.96% ($24,963) → rec= 2.00%  Δ=-22.96pp  [TRIM]
- `12:17:15`     TMUS      src=earnings_pead       hor=day_30  w=0.700 cur= 24.90% ($24,900) → rec= 2.00%  Δ=-22.90pp  [TRIM]
- `12:17:15`     SHOP      src=short_squeeze       hor=day_30  w=0.700 cur= 24.90% ($24,896) → rec= 2.00%  Δ=-22.90pp  [TRIM]
- `12:17:15`     LIN       src=short_squeeze       hor=day_30  w=0.700 cur= 24.89% ($24,888) → rec= 2.00%  Δ=-22.89pp  [TRIM]
- `12:17:15`     TSLA      src=short_squeeze       hor=day_30  w=0.700 cur= 24.62% ($24,622) → rec= 2.00%  Δ=-22.62pp  [TRIM]
- `12:17:15`     LLY       src=short_squeeze       hor=day_30  w=0.700 cur= 24.08% ($24,083) → rec= 2.00%  Δ=-22.08pp  [TRIM]
- `12:17:15`     MELI      src=short_squeeze       hor=day_30  w=0.700 cur= 24.05% ($24,051) → rec= 2.00%  Δ=-22.05pp  [TRIM]
- `12:17:15` 
- `12:17:15`   Total current exposure: 272.31%
- `12:17:15`   Total recommended:      22.00%
- `12:17:15`   Action distribution:    {'TRIM': 11}
# 4) Verify sizing.html live

- `12:17:24`   ✓ status=200, size=18,581b
- `12:17:24`     ✓ title
- `12:17:24`     ✓ nav active
- `12:17:24`     ✓ call banner
- `12:17:24`     ✓ position table
- `12:17:24`     ✓ setup table
- `12:17:24`     ✓ loads sizer-v2.json
- `12:17:24`     ✓ renderPositions fn
- `12:17:24`     ✓ auto-refresh
