
# 1) Patch tier-classifier source — FMP /stable migration

- `14:35:14`   ✓ FMP_BASE: /api/v3 → /stable
- `14:35:14`   ✓ fetch_fundamentals: migrated to /stable endpoints + new field names
- `14:35:14`   ✓ Patched source written: aws/lambdas/justhodl-theme-tier-classifier/source/lambda_function.py

# 2) Build deployment zip

- `14:35:14`   zip size: 21,120b

# 3) Redeploy

- `14:35:18`   ✓ deployed, mod=2026-05-05T14:35:15.000+0000

# 4) Smoke invoke

- `14:36:05`   status=200 duration=46.9s
- `14:36:05`   ── Response body ──
- `14:36:05`     n_themes_classified: 47
- `14:36:05`     n_unique_tickers: 349
- `14:36:05`     n_fundamentals_ok: 295
- `14:36:05`     n_deep_asymmetry: 33
- `14:36:05`     n_asymmetric: 142
- `14:36:05`     top_asymmetric: ['PYPL', 'CSR', 'AIN', 'RC', 'ARRY']
- `14:36:05`     mu_grade: ['PYPL', 'AIN', 'RC', 'ARRY', 'JKS']
- `14:36:05`     duration_s: 45.9
- `14:36:05`   ── Log tail (last 25) ──
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [fmp] /profile all_retries_failed err=HTTP429
- `14:36:05`     [tier-classifier] fetched 295 ok / 54 failed in 45.7s
- `14:36:05`     [tier-classifier] USO skipped: only 1 fundamentals
- `14:36:05`     [tier-classifier] SLV skipped: only 1 fundamentals
- `14:36:05`     [tier-classifier] BITO skipped: only 1 fundamentals
- `14:36:05`     [tier-classifier] ETHE skipped: only 1 fundamentals
- `14:36:05`     [tier-classifier] DBB skipped: only 1 fundamentals
- `14:36:05`     [tier-classifier] DBA skipped: only 1 fundamentals
- `14:36:05`     [tier-classifier] BCI skipped: only 1 fundamentals
- `14:36:05`     [tier-classifier] GLD skipped: only 1 fundamentals
- `14:36:05`     [tier-classifier] wrote 306322b to data/theme-tiers.json
- `14:36:05`     [tier-classifier] top asymmetric: [('PYPL', 93.2), ('CSR', 92.1), ('AIN', 89.1), ('RC', 88.6), ('ARRY', 85.4), ('JKS', 85.0), ('UPS', 84.5), ('UPS', 84.0)]
- `14:36:05`     [tier-classifier] MU-grade (mcap_to_rev<=3): [('PYPL', 1.234), ('AIN', 1.364), ('RC', 0.625), ('ARRY', 0.99), ('JKS', 0.005)]
- `14:36:05`     END RequestId: 276341b1-0e4b-4755-a629-b6899737913e
- `14:36:05`     REPORT RequestId: 276341b1-0e4b-4755-a629-b6899737913e	Duration: 46052.36 ms	Billed Duration: 46520 ms	Memory Size: 1024 MB	Max Memory Used: 106 MB	Init Duration: 467.57 ms	

# 5) Verify S3 output

- `14:36:06`   S3 size: 306,322b
- `14:36:06`   S3 last_modified: 2026-05-05 14:36:06+00:00
- `14:36:06`   v: 1.0
- `14:36:06`   n_themes_classified: 47
- `14:36:06`   n_total_classifications: 400
- `14:36:06`   n_deep_asymmetry: 33
- `14:36:06`   n_asymmetric: 142
- `14:36:06`   
- `14:36:06`   ── Top 12 asymmetric leaderboard ──
- `14:36:06`     PYPL   (FDN   EMERGING     ) tier=2 score= 93.2 flag=DEEP_ASYMMETRY   mcap_to_rev=1.23   p_s=1.22
- `14:36:06`     CSR    (REZ   EMERGING     ) tier=2 score= 92.1 flag=DEEP_ASYMMETRY   mcap_to_rev=3.23   p_s=3.23
- `14:36:06`     AIN    (ROBO  EMERGING     ) tier=2 score= 89.1 flag=DEEP_ASYMMETRY   mcap_to_rev=1.36   p_s=1.37
- `14:36:06`     RC     (REM   EMERGING     ) tier=2 score= 88.6 flag=DEEP_ASYMMETRY   mcap_to_rev=0.62   p_s=0.63
- `14:36:06`     ARRY   (TAN   EMERGING     ) tier=2 score= 85.4 flag=DEEP_ASYMMETRY   mcap_to_rev=0.99   p_s=0.99
- `14:36:06`     JKS    (TAN   EMERGING     ) tier=2 score= 85.0 flag=DEEP_ASYMMETRY   mcap_to_rev=0.01   p_s=0.03
- `14:36:06`     UPS    (BOTZ  EMERGING     ) tier=2 score= 84.5 flag=DEEP_ASYMMETRY   mcap_to_rev=0.94   p_s=0.94
- `14:36:06`     UPS    (XLI   EMERGING     ) tier=2 score= 84.0 flag=DEEP_ASYMMETRY   mcap_to_rev=0.94   p_s=0.94
- `14:36:06`     CLF    (SLX   EXTENDED     ) tier=1 score= 82.2 flag=DEEP_ASYMMETRY   mcap_to_rev=0.31   p_s=0.32
- `14:36:06`     CSIQ   (TAN   EMERGING     ) tier=2 score= 82.2 flag=DEEP_ASYMMETRY   mcap_to_rev=0.20   p_s=0.20
- `14:36:06`     PYPL   (BLOK  EMERGING     ) tier=2 score= 82.0 flag=DEEP_ASYMMETRY   mcap_to_rev=1.23   p_s=1.22
- `14:36:06`     CNH    (AIRR  EMERGING     ) tier=1 score= 81.8 flag=DEEP_ASYMMETRY   mcap_to_rev=0.72   p_s=0.72
- `14:36:06`   
- `14:36:06`   ── MU-grade leaderboard (mcap_to_rev <= 3) ──
- `14:36:06`     PYPL   (FDN   EMERGING     ) score= 93.2 mcap_to_rev=1.23  
- `14:36:06`     AIN    (ROBO  EMERGING     ) score= 89.1 mcap_to_rev=1.36  
- `14:36:06`     RC     (REM   EMERGING     ) score= 88.6 mcap_to_rev=0.62  
- `14:36:06`     ARRY   (TAN   EMERGING     ) score= 85.4 mcap_to_rev=0.99  
- `14:36:06`     JKS    (TAN   EMERGING     ) score= 85.0 mcap_to_rev=0.01  
- `14:36:06`     UPS    (BOTZ  EMERGING     ) score= 84.5 mcap_to_rev=0.94  
- `14:36:06`     UPS    (XLI   EMERGING     ) score= 84.0 mcap_to_rev=0.94  
- `14:36:06`     CLF    (SLX   EXTENDED     ) score= 82.2 mcap_to_rev=0.31  
- `14:36:06`     CSIQ   (TAN   EMERGING     ) score= 82.2 mcap_to_rev=0.20  
- `14:36:06`     PYPL   (BLOK  EMERGING     ) score= 82.0 mcap_to_rev=1.23  
- `14:36:06`     CNH    (AIRR  EMERGING     ) score= 81.8 mcap_to_rev=0.72  
- `14:36:06`     PCAR   (PAVE  EMERGING     ) score= 81.8 mcap_to_rev=2.21  
- `14:36:06`   
- `14:36:06`   ── Tier-2 leaderboard ──
- `14:36:06`     PYPL   (FDN  ) score= 93.2 mcap_to_rev=1.23
- `14:36:06`     CSR    (REZ  ) score= 92.1 mcap_to_rev=3.23
- `14:36:06`     AIN    (ROBO ) score= 89.1 mcap_to_rev=1.36
- `14:36:06`     RC     (REM  ) score= 88.6 mcap_to_rev=0.62
- `14:36:06`     ARRY   (TAN  ) score= 85.4 mcap_to_rev=0.99
- `14:36:06`     JKS    (TAN  ) score= 85.0 mcap_to_rev=0.01
- `14:36:06`     UPS    (BOTZ ) score= 84.5 mcap_to_rev=0.94
- `14:36:06`     UPS    (XLI  ) score= 84.0 mcap_to_rev=0.94
- `14:36:06`     CSIQ   (TAN  ) score= 82.2 mcap_to_rev=0.20
- `14:36:06`     PYPL   (BLOK ) score= 82.0 mcap_to_rev=1.23
- `14:36:06`   
- `14:36:06`   ── Sample theme: SMH (Semiconductors, phase=ACCELERATING) ──
- `14:36:06`     theme medians: P/S=14.2177154284592 P/E=40.97014925373134 mcap/rev=15.699
- `14:36:06`     n with stats: P/S=9 P/E=9 mcr=9
- `14:36:06`       NVDA   tier=1 score= 27.7 P/S=22.33 mcap_to_rev=22.32
- `14:36:06`       AVGO   tier=1 score=  8.2 P/S=29.26 mcap_to_rev=29.29
- `14:36:06`       TSM    tier=1 score= 61.8 P/S=14.22 mcap_to_rev=2.52
- `14:36:06`       AMD    tier=1 score= 22.6 P/S=16.60 mcap_to_rev=16.60
- `14:36:06`       QCOM   tier=1 score= 73.9 P/S=4.22 mcap_to_rev=4.27
- `14:36:06`       ASML   tier=2 score= 37.7 P/S=13.84 mcap_to_rev=16.27
- `14:36:06`       AMAT   tier=2 score= 43.8 P/S=11.39 mcap_to_rev=11.38
- `14:36:06`       LRCX   tier=2 score= 35.0 P/S=15.71 mcap_to_rev=15.70
- `14:36:06`       MU     tier=2 score= 43.9 P/S=12.48 mcap_to_rev=12.45