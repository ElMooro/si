# Trigger and verify rewritten earnings tracker

**Status:** success  
**Duration:** 31.9s  
**Finished:** 2026-05-04T00:00:22+00:00  

## Log
## 1. Lambda config sanity

- `23:59:51`   runtime: python3.12
- `23:59:51`   memory: 512 MB
- `23:59:51`   timeout: 600s
- `23:59:51`   last_modified: 2026-05-03T23:58:02.000+0000
- `23:59:51`   env vars: ['S3_BUCKET', 'FMP_KEY', 'POLYGON_KEY', 'S3_KEY', 'MAX_PARALLEL']
## 2. Trigger Lambda

- `00:00:22`   status: 200
- `00:00:22`   duration: 31.6s
- `00:00:22`   response: {"statusCode": 200, "body": "{\"n_upcoming\": 47, \"n_recent\": 52, \"n_pead\": 10, \"duration_s\": 30.82}"}
## 3. Verify S3 output

- `00:00:22`   generated_at: 2026-05-04T00:00:22.562982+00:00
- `00:00:22`   duration_s: 30.82
- `00:00:22`   watchlist_size: 170
- `00:00:22`   n_upcoming: 47
- `00:00:22`   n_recent: 52
- `00:00:22`   n_pead: 10
- `00:00:22`   beat_rate: 48.1%
- `00:00:22`   median_1d_return: -0.12%
- `00:00:22`   pct_positive_reactions: 43.9%
## 4. Sample upcoming earnings

- `00:00:22`     PLTR   2026-05-04 AMC  EPS_est=0.22 — Palantir Technologies Inc.
- `00:00:22`     VRTX   2026-05-04 AMC  EPS_est=3.67 — Vertex Pharmaceuticals Incorporated
- `00:00:22`     FANG   2026-05-04 AMC  EPS_est=3.55 — Diamondback Energy, Inc.
- `00:00:22`     AMD    2026-05-05 AMC  EPS_est=1.06 — Advanced Micro Devices, Inc.
- `00:00:22`     ETN    2026-05-05 BMO  EPS_est=2.74 — Eaton Corporation, PLC
- `00:00:22`     SHOP   2026-05-05 BMO  EPS_est=0.22 — Shopify Inc.
- `00:00:22`     PFE    2026-05-05 BMO  EPS_est=0.71 — Pfizer, Inc.
- `00:00:22`     KKR    2026-05-05 BMO  EPS_est=1.12 — KKR & Co. Inc.
## 5. Sample recent results (PEAD)

- `00:00:22`     MSFT   filed:2026-04-29 eps_actual=4.28 yoy=-17.37% 1d=-3.93% 5d=None% label=NEGATIVE_DRIFT
- `00:00:22`     QCOM   filed:2026-04-29 eps_actual=6.92 yoy=146.26% 1d=15.12% 5d=None% label=STRONG_POSITIVE_DRIFT
- `00:00:22`     TMUS   filed:2026-04-28 eps_actual=2.28 yoy=19.37% 1d=6.13% 5d=None% label=STRONG_POSITIVE_DRIFT
- `00:00:22`     NOW    filed:2026-04-23 eps_actual=0.45 yoy=109.36% 1d=6.36% 5d=4.16% label=STRONG_POSITIVE_DRIFT
- `00:00:22`     ELV    filed:2026-04-22 eps_actual=8.03 yoy=214.9% 1d=5.51% 5d=14.76% label=STRONG_POSITIVE_DRIFT
- `00:00:22`     GE     filed:2026-04-21 eps_actual=1.82 yoy=-24.48% 1d=-3.64% 5d=0.86% label=NEGATIVE_DRIFT
- `00:00:22`     DHR    filed:2026-04-21 eps_actual=1.45 yoy=-14.2% 1d=-5.4% 5d=-8.0% label=NEGATIVE_DRIFT
- `00:00:22`     NOC    filed:2026-04-21 eps_actual=6.16 yoy=-38.28% 1d=-3.52% 5d=-5.45% label=NEGATIVE_DRIFT
