
# 1) Tune Lambda config for higher volume

- `18:39:34`     ✓ MAX_FILINGS=3000, days=7, workers=12, mem=1536MB, timeout=900s, MIN_BUY=$5K

# 2) Async invoke

- `18:39:34`     initial S3 mod: 2026-05-05 18:26:29+00:00
- `18:39:34`     invoked async (status: 202)

# 3) Poll S3 every 30s for fresh output (12 min budget)

- `18:40:04`     [30s] still old
- `18:40:35`     [60s] still old
- `18:41:05`     [90s] still old
- `18:41:35`     [120s] still old
- `18:42:05`     [150s] still old
- `18:42:35`     [181s] still old
- `18:43:05`     [211s] still old
- `18:43:36`     [241s] still old
- `18:44:06`     [271s] still old
- `18:44:36`     [301s] still old
- `18:45:06`     [332s] still old
- `18:45:36`     [362s] still old
- `18:46:07`     [392s] still old
- `18:46:37`     [422s] still old
- `18:47:07`     [452s] still old
- `18:47:37`     [483s] still old
- `18:48:07`     [513s] still old
- `18:48:38`     [543s] still old
- `18:49:08`     [573s] still old
- `18:49:38`     [603s] still old
- `18:50:08`     ✓ S3 updated at 2026-05-05 18:50:08+00:00 (elapsed: 633s, size: 43,345b)

# 4) CloudWatch tail (latest stream)

- `18:50:09`     stream: 2026/05/05/[$LATEST]0ce79b282ca7484587e7433b514a8a51  events: 7
- `18:50:09`       INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e4ab553846c4e081013ff7d1d608a5358d5b956bb5b81c83c66d2a31da8f6244
- `18:50:09`       START RequestId: 8d8b2f24-81f5-49ac-b46f-6d8374949dec Version: $LATEST
- `18:50:09`       [insider-cluster] starting v2, lookback=30d, max_filings=3000
- `18:50:09`       [insider-cluster] pulling daily index for 7 biz days: 2026-04-27 → 2026-05-05
- `18:50:09`       [insider-cluster] found 7398 Form 4 filings (0 index errors)
- `18:50:09`       [insider-cluster] sampling top 3000 most recent for parsing
- `18:50:09`       [insider-cluster] parse deadline: 838s from start

# 5) Read S3 + dump top 20 clusters

- `18:50:09`     schema: 2.0
- `18:50:09`     generated_at: 2026-05-05T18:50:07.885475+00:00
- `18:50:09`     stats: {"n_form4_filings_scanned": 7398, "n_form4_parsed": 3000, "n_buy_transactions": 218, "n_unique_tickers": 55, "n_clusters": 22, "n_strong_signals": 8, "n_smart_money_dual": 4, "n_ceo_conviction": 1, "n_cluster_buys": 5, "n_contrarian_clusters": 7}
- `18:50:09`     n_clusters: 22
- `18:50:09`   
- `18:50:09`       Ticker   Score Signal                 Ins     $Total  %52H Mcap     Sector
- `18:50:09`       SRAD      90.8 executive_cluster        7 $    4.67M  -58% $4.0B    Technology
- `18:50:09`       SPGI      86.2 executive_cluster        3 $    2.58M  -27% $125.2B  Financial Services
- `18:50:09`       SUNE      80.8 smart_money_dual         2 $    1.20M  -57% $5M      Industrials
- `18:50:09`       FND       78.5 smart_money_dual         2 $    0.37M  -48% $5.2B    Consumer Cyclical
- `18:50:09`       OPCH      78.5 smart_money_dual         2 $    0.59M  -42% $3.4B    Healthcare
- `18:50:09`       CSGP      77.2 ceo_conviction           1 $    2.51M  -64% $14.1B   Real Estate
- `18:50:09`       EPAM      73.5 smart_money_dual         6 $    0.04M  -51% $5.8B    Technology
- `18:50:09`       NWBI      70.0 executive_cluster        3 $    0.19M   -2% $2.1B    Financial Services
- `18:50:09`       PSUS      66.0 cluster_buy              8 $  311.39M   -3% $1.7B    
- `18:50:09`       NONE      59.0 cluster_buy              3 $   17.20M   +0% ?        
- `18:50:09`       FGBI      52.8 lone_buy                 2 $    2.00M   -7% $148M    Financial Services
- `18:50:09`       AVLN      52.5 lone_buy                 2 $   15.00M  -11% $1.1B    Healthcare
- `18:50:09`       PS        48.0 lone_buy                 1 $   19.02M  -16% $12.8B   Financial Services
- `18:50:09`       GLND      46.5 lone_buy                 2 $    0.28M  -87% $76M     Energy
- `18:50:09`       CECO      44.2 lone_buy                 1 $    1.10M   -1% $3.1B    Industrials
- `18:50:09`       AUID      41.5 lone_buy                 2 $    0.19M  -82% $16M     Technology
- `18:50:09`       NMM       38.0 lone_buy                 1 $    0.25M   -1% $2.2B    Industrials
- `18:50:09`       NWFL      36.5 lone_buy                 2 $    0.02M   -8% $274M    Financial Services
- `18:50:09`       AXR       34.0 lone_buy                 1 $    0.22M   -4% $148M    Real Estate
- `18:50:09`       XZO       33.0 lone_buy                 1 $    0.06M  -30% $1.6B    Financial Services
- `18:50:09`   
- `18:50:09`     ── Top cluster — full structure ──
- `18:50:09`       ticker: SRAD
- `18:50:09`       company: Sportradar Group AG
- `18:50:09`       score: 90.8
- `18:50:09`       signal_type: executive_cluster
- `18:50:09`       n_insiders: 7
- `18:50:09`       n_transactions: 10
- `18:50:09`       total_value: 4668317.9
- `18:50:09`       avg_price: 13.04
- `18:50:09`       first_buy: 2026-04-30
- `18:50:09`       last_buy: 2026-05-01
- `18:50:09`       has_ceo: True
- `18:50:09`       has_cfo: False
- `18:50:09`       has_chairman: False
- `18:50:09`       rationale: CEO (+6 other) bought $4.67M of SRAD over 1d at $13.04 avg — stock 58% off 52w high
- `18:50:09`       insiders (7):
- `18:50:09`         • Koerl Carsten                    Director, Chief Executive Officer   $ 3,342,856
- `18:50:09`         • Bigley Deirdre Mary              Director                            $    49,526
- `18:50:09`         • Fleet George                     Director                            $    99,930
- `18:50:09`         • KURTZ WILLIAM                    Director                            $   103,786
- `18:50:09`         • Ramanathan Rajani                Director                            $   100,000
- `18:50:09`         • Walder Marc                      Director                            $   842,820
- `18:50:09`         • YABUKI JEFFERY W                 Director                            $   129,400