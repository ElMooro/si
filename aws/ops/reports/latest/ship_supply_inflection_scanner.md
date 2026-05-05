
# 1) Build deployment zip

- `14:12:53`   zip size: 26,936b

# 2) Create or update Lambda

- `14:12:53`   Lambda justhodl-supply-inflection-scanner does not exist — creating
- `14:12:55`   ✅ Lambda deployed

# 3) Schedule daily 07:00 UTC

- `14:12:55`   Rule put: justhodl-supply-inflection-scanner-daily (cron(0 7 * * ? *))
- `14:12:55`   Lambda invoke permission added
- `14:12:55`   Target attached

# 4) Smoke invoke (LogType=Tail)

- `14:12:57`   status: 200, duration: 2.5s
- `14:12:57`   
- `14:12:57`   ── Response body ──
- `14:12:57`     n_signals: 17
- `14:12:57`     n_strong_tightening: 6
- `14:12:57`     n_tightening: 8
- `14:12:57`     top_signals: ['MEMORY_DEMAND', 'LITHIUM', 'RARE_EARTH', 'STEEL_BASKET', 'URANIUM']
- `14:12:57`     top_inflecting_themes: ['SOXX', 'LIT', 'REMX', 'AIQ', 'BOTZ']
- `14:12:57`     duration_s: 1.4
- `14:12:57`   
- `14:12:57`   ── Log tail ──
- `14:12:57`     START RequestId: d90a6db6-c232-4b09-99ec-52d3708ba558 Version: $LATEST
- `14:12:57`     [supply-inflection] scanning 22 signals
- `14:12:57`     [fetch-fail] ISM_PMI bars=19[fred] INDPRO HTTPError 500 body={"error_code":500,"error_message":"Internal Server Error"}
- `14:12:57`     [fetch-fail] INDUSTRIAL_PROD bars=0
- `14:12:57`     [fred] IPN213111N HTTPError 500 body={"error_code":500,"error_message":"Internal Server Error"}
- `14:12:57`     [fetch-fail] RIG_COUNT bars=0
- `14:12:57`     [fred] DTWEXBGS HTTPError 500 body={"error_code":500,"error_message":"Internal Server Error"}
- `14:12:57`     [fetch-fail] DOLLAR_INDEX bars=0
- `14:12:57`     [fred] TRUCKD11 HTTPError 500 body={"error_code":500,"error_message":"Internal Server Error"}
- `14:12:57`     [fetch-fail] TRUCK_TONNAGE bars=0
- `14:12:57`     [supply-inflection] fetched 17 ok / 5 failed in 1.4s
- `14:12:57`     [supply-inflection] wrote 62694b to data/supply-inflection.json
- `14:12:57`     [supply-inflection] top tightening: ['MEMORY_DEMAND:99.9', 'LITHIUM:99.6', 'RARE_EARTH:99.5', 'STEEL_BASKET:94.3', 'URANIUM:93.7']
- `14:12:57`     [supply-inflection] top theme inflections: [('SOXX', 99.9), ('LIT', 99.6), ('REMX', 99.6), ('AIQ', 98.4), ('BOTZ', 98.4)]
- `14:12:57`     END RequestId: d90a6db6-c232-4b09-99ec-52d3708ba558
- `14:12:57`     REPORT RequestId: d90a6db6-c232-4b09-99ec-52d3708ba558	Duration: 1569.66 ms	Billed Duration: 2243 ms	Memory Size: 1024 MB	Max Memory Used: 112 MB	Init Duration: 672.92 ms	

# 5) Verify S3 output

- `14:12:58`   S3 size: 62,694b
- `14:12:58`   S3 last_modified: 2026-05-05 14:12:58+00:00
- `14:12:58`   v: 1.0
- `14:12:58`   method: supply_inflection_scanner_v1
- `14:12:58`   n_signals: 17
- `14:12:58`   n_strong_tightening: 6
- `14:12:58`   n_tightening: 8
- `14:12:58`   n_easing: 5
- `14:12:58`   
- `14:12:58`   ── Top 8 tightening signals ──
- `14:12:58`     MEMORY_DEMAND             MU       score= 99.9 flag=STRONG_TIGHTENING    themes=['SMH', 'SOXX', 'AIQ', 'BOTZ']
- `14:12:58`     LITHIUM                   LIT      score= 99.6 flag=STRONG_TIGHTENING    themes=['LIT', 'REMX']
- `14:12:58`     RARE_EARTH                REMX     score= 99.5 flag=STRONG_TIGHTENING    themes=['REMX', 'ITA']
- `14:12:58`     STEEL_BASKET              SLX      score= 94.3 flag=STRONG_TIGHTENING    themes=['SLX', 'PICK']
- `14:12:58`     URANIUM                   URA      score= 93.7 flag=STRONG_TIGHTENING    themes=['URA', 'URNM', 'NLR']
- `14:12:58`     AI_INFRA_PROXY            NVDA     score= 92.3 flag=STRONG_TIGHTENING    themes=['AIQ', 'BOTZ', 'ROBO', 'SMH']
- `14:12:58`     OIL_BRENT                 BNO      score= 71.6 flag=TIGHTENING           themes=['XLE', 'XOP']
- `14:12:58`     OIL_WTI                   USO      score= 63.3 flag=TIGHTENING           themes=['XLE', 'XOP', 'OIH', 'AMLP']
- `14:12:58`   
- `14:12:58`   ── Top 8 inflecting themes ──
- `14:12:58`     SOXX   score= 99.9 n_strong=1 n_tightening=1
- `14:12:58`     LIT    score= 99.6 n_strong=1 n_tightening=1
- `14:12:58`     REMX   score= 99.6 n_strong=2 n_tightening=2
- `14:12:58`     AIQ    score= 98.4 n_strong=2 n_tightening=2
- `14:12:58`     BOTZ   score= 98.4 n_strong=2 n_tightening=2
- `14:12:58`     SMH    score= 98.4 n_strong=2 n_tightening=2
- `14:12:58`     SLX    score= 94.3 n_strong=1 n_tightening=1
- `14:12:58`     URA    score= 93.7 n_strong=1 n_tightening=1
- `14:12:58`   
- `14:12:58`   ── Sample signal detail (top 1) ──
- `14:12:58`     MEMORY_DEMAND (MU): score=99.9 flag=STRONG_TIGHTENING
- `14:12:58`     metrics: pct_30d=72.09 pct_90d=66.12 pct_180d=164.44
- `14:12:58`              pctl_365d=99.6 vol_90d=71.23 latest=630.25