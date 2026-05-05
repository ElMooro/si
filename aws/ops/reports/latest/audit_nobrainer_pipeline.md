
# 1) Lambda + S3 freshness audit

- `16:09:31`   
- `16:09:31`   ── L1 Theme Detector: justhodl-theme-detector ──
- `16:09:32`     state: Active  mem=1024MB  timeout=300s
- `16:09:32`     modified: 2026-05-05T13:40:15.000+0000
- `16:09:32`     env keys: ['POLYGON_KEY']
- `16:09:32`     S3 data/themes-detected.json: 57,869b  modified 2026-05-05 13:49:04+00:00
- `16:09:32`   
- `16:09:32`   ── L2 Supply Inflection: justhodl-supply-inflection-scanner ──
- `16:09:32`     state: Active  mem=1024MB  timeout=300s
- `16:09:32`     modified: 2026-05-05T14:20:30.000+0000
- `16:09:32`     env keys: ['FRED_KEY', 'POLYGON_KEY']
- `16:09:32`     S3 data/supply-inflection.json: 68,210b  modified 2026-05-05 14:20:35+00:00
- `16:09:32`   
- `16:09:32`   ── L3 Tier Classifier: justhodl-theme-tier-classifier ──
- `16:09:32`     state: Active  mem=1024MB  timeout=600s
- `16:09:32`     modified: 2026-05-05T14:35:15.000+0000
- `16:09:32`     env keys: ['FMP_KEY']
- `16:09:33`     S3 data/theme-tiers.json: 306,322b  modified 2026-05-05 14:36:06+00:00
- `16:09:33`   
- `16:09:33`   ── L4 Asymmetric Hunter: justhodl-asymmetric-hunter ──
- `16:09:33`     state: Active  mem=1024MB  timeout=600s
- `16:09:33`     modified: 2026-05-05T14:44:57.411+0000
- `16:09:33`     env keys: ['FMP_KEY']
- `16:09:33`     S3 data/nobrainers.json: 456,897b  modified 2026-05-05 15:54:23+00:00
- `16:09:33`   
- `16:09:33`   ── L5 Nobrainer Rationale: justhodl-nobrainer-rationale ──
- `16:09:33`     state: Active  mem=512MB  timeout=600s
- `16:09:33`     modified: 2026-05-05T14:53:58.000+0000
- `16:09:33`     env keys: ['N_THESES', 'MIN_SCORE', 'N_DIGEST']
- `16:09:33`     S3 data/nobrainers-rationale.json: 13,509b  modified 2026-05-05 15:54:24+00:00
- `16:09:33`   
- `16:09:33`   ── L6 Nobrainer Tracker: justhodl-nobrainer-tracker ──
- `16:09:33`     state: Active  mem=512MB  timeout=300s
- `16:09:33`     modified: 2026-05-05T14:56:09.333+0000
- `16:09:33`     env keys: ['MAX_LOGS_PER_RUN', 'POLYGON_KEY', 'RECONFIRM_HOURS', 'MIN_TRACK_SCORE', 'SCORE_DELTA_TRIGGER']

# 2) Force-invoke L4 asymmetric-hunter

- `16:09:37`     status: 200  body keys: ['statusCode', 'body']
- `16:09:37`     inner keys: ['n_candidates_scored', 'n_tier_a_nobrainer', 'n_mu_grade', 'duration_s']
- `16:09:37`     ── tail logs (last 4kb) ──
- `16:09:37`       START RequestId: 2df1247c-eeb3-4430-822e-b8f6ea3a6900 Version: $LATEST
- `16:09:37`       [hunter] Layer 4 — asymmetric-hunter starting
- `16:09:37`       [hunter] inputs loaded: {'themes_detected': True, 'supply_inflection': True, 'theme_tiers': True}
- `16:09:37`       [hunter] candidates: 400
- `16:09:37`       [hunter] fetching earnings for 288 unique tickers (max_workers=6)
- `16:09:37`       [hunter] earnings fetched in 2.3s (cache size: 288)
- `16:09:37`       [hunter] wrote 456897b to data/nobrainers.json
- `16:09:37`       [hunter] tier_a=9 tier_b=33 tier_c=11 mu_grade=25
- `16:09:37`       [hunter] TOP 5: [('TX', 'SLX', 86.5, 'TIER_A_NOBRAINER'), ('USAR', 'REMX', 85.8, 'TIER_A_NOBRAINER'), ('CSTM', 'REMX', 83.0, 'TIER_A_NOBRAINER'), ('MT', 'SLX', 82.1, 'TIER_A_NOBRAINER'), ('APA', 'XOP', 81.8, 'TIER_A_NOBRAINER')]
- `16:09:37`       END RequestId: 2df1247c-eeb3-4430-822e-b8f6ea3a6900
- `16:09:37`       REPORT RequestId: 2df1247c-eeb3-4430-822e-b8f6ea3a6900	Duration: 2819.97 ms	Billed Duration: 3340 ms	Memory Size: 1024 MB	Max Memory Used: 108 MB	Init Duration: 519.64 ms	

# 3) Force-invoke L5 nobrainer-rationale (top_n=3)

- `16:09:39`     status: 200  body keys: ['statusCode', 'body']
- `16:09:39`     inner keys: ['n_theses', 'n_claude_ok', 'n_claude_fail', 'duration_s']
- `16:09:39`     n_theses: 10

# 4) Force-invoke L6 nobrainer-tracker

- `16:09:40`     status: 200  body keys: ['statusCode', 'body']
- `16:09:40`     inner: {"n_logged": 0, "n_skipped": 24, "n_errors": 1, "n_total_ever": 24, "duration_s": 0.8}

# 5) Summary tally

- `16:09:42`   ✅ all 6 layers + S3 outputs present