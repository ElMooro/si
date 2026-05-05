
# 1) Async-invoke insider-cluster-scanner

- `18:24:41`     ✓ env: MAX_FILINGS=500, N_BUSINESS_DAYS_INDEX=3, N_WORKERS=8
- `18:24:41`     invoking ASYNC (Event)…
- `18:24:41`     status: 202 (202 = queued)

# 2) Poll S3 every 30s for fresh output

- `18:24:41`     initial S3 mod: 2026-05-05 18:22:44+00:00
- `18:25:11`     [30s] still old: 2026-05-05 18:22:44+00:00
- `18:25:41`     [60s] still old: 2026-05-05 18:22:44+00:00
- `18:26:11`     [90s] still old: 2026-05-05 18:22:44+00:00
- `18:26:42`     ✓ S3 updated at 2026-05-05 18:26:29+00:00 (elapsed: 121s, size: 5,299b)

# 3) Pull last CloudWatch log stream

- `18:26:42`     stream: 2026/05/05/[$LATEST]a2c0f8ccfffc40099e0042df1a55a2e1
- `18:26:42`     total events: 18
- `18:26:42`   
- `18:26:42`     ── log content (last 60 lines) ──
- `18:26:42`       INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e4ab553846c4e081013ff7d1d608a5358d5b956bb5b81c83c66d2a31da8f6244
- `18:26:42`       START RequestId: 8b249fa9-d7a5-47e7-b281-28f5c305340a Version: $LATEST
- `18:26:42`       [insider-cluster] starting v2, lookback=30d, max_filings=500
- `18:26:42`       [insider-cluster] pulling daily index for 3 biz days: 2026-05-01 → 2026-05-05
- `18:26:42`       [insider-cluster] found 3442 Form 4 filings (0 index errors)
- `18:26:42`       [insider-cluster] sampling top 500 most recent for parsing
- `18:26:42`       [insider-cluster] parse deadline: 539s from start
- `18:26:42`       [insider-cluster] parsed 500 filings, extracted 26 buy transactions (477 failed)
- `18:26:42`       [insider-cluster] 16 unique tickers
- `18:26:42`       [insider-cluster] 3 clusters meeting threshold
- `18:26:42`       [insider-cluster] enriching top 3 with FMP fundamentals
- `18:26:42`       [insider-cluster] wrote 5,299b in 106.3s
- `18:26:42`       [insider-cluster] strong=1 smart_money=0 ceo_conv=1 cluster=0 contrarian=1
- `18:26:42`       [insider-cluster] TOP: CSGP     score= 77.2 ceo_conviction         $ 2.51M 1-ins
- `18:26:42`       [insider-cluster] TOP: NWBI     score= 49.5 exec_pair              $ 0.17M 2-ins
- `18:26:42`       [insider-cluster] TOP: PS       score= 48.0 lone_buy               $19.02M 1-ins
- `18:26:42`       END RequestId: 8b249fa9-d7a5-47e7-b281-28f5c305340a
- `18:26:42`       REPORT RequestId: 8b249fa9-d7a5-47e7-b281-28f5c305340a	Duration: 106333.39 ms	Billed Duration: 106991 ms	Memory Size: 1024 MB	Max Memory Used: 103 MB	Init Duration: 657.32 ms

# 4) Read S3 output

- `18:26:42`     schema: 2.0  method: insider_cluster_scanner_v2
- `18:26:42`     generated_at: 2026-05-05T18:26:28.445699+00:00
- `18:26:42`     stats: {"n_form4_filings_scanned": 3442, "n_form4_parsed": 500, "n_buy_transactions": 26, "n_unique_tickers": 16, "n_clusters": 3, "n_strong_signals": 1, "n_smart_money_dual": 0, "n_ceo_conviction": 1, "n_cluster_buys": 0, "n_contrarian_clusters": 1}
- `18:26:42`     n_clusters: 3
- `18:26:42`   
- `18:26:42`     ── Top 15 clusters ──
- `18:26:42`       Ticker   Score Signal                   Ins     $Total  %52H Mcap     Sector
- `18:26:42`       CSGP      77.2 ceo_conviction             1 $    2.51M  -65% $14.1B   Real Estate
- `18:26:42`       NWBI      49.5 exec_pair                  2 $    0.17M   -2% $2.1B    Financial Services
- `18:26:42`       PS        48.0 lone_buy                   1 $   19.02M  -15% $13.0B   Financial Services