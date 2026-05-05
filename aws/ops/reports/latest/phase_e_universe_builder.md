
# 0) Write Lambda source

- `22:30:01`     wrote aws/lambdas/justhodl-universe-builder/source/lambda_function.py: 7339 chars
- `22:30:01`     ✓ valid python

# 1) Build zip + create/update Lambda

- `22:30:01`     zip: 7,475b
- `22:30:02`     ✓ created new Lambda
- `22:30:04`     ready: mem=1024MB to=300s

# 2) Schedule rate(4 hours)

- `22:30:04`     ✓ permission added

# 3) Smoke invoke (this will take ~3-4 minutes)

- `22:30:16`     status: 200, dur: 12.0s
- `22:30:16`     body: {"statusCode": 200, "body": "{\"n_total\": 231, \"duration_s\": 10.8, \"n_by_sector\": 12}"}
- `22:30:16`       START RequestId: f8f2a938-4187-4c8f-8cc7-81f97dcdce45 Version: $LATEST
- `22:30:16`       [universe] starting v1.0, max_tickers=1800, min_mcap=$0.20B
- `22:30:16`       [universe] FMP stock-list returned 48940 tickers
- `22:30:16`       [universe] pre-filter retained 36918 candidates
- `22:30:16`       [universe] capped to 1800 for enrichment budget
- `22:30:16`       [universe] enriched: 231 stocks, statuses: {'ok': 231, 'no_quote': 0, 'below_mcap': 1569, 'deadline': 0}
- `22:30:16`       [universe] runtime: 10.8s
- `22:30:16`       [universe] wrote 76,678b to data/universe.json
- `22:30:16`       END RequestId: f8f2a938-4187-4c8f-8cc7-81f97dcdce45
- `22:30:16`       REPORT RequestId: f8f2a938-4187-4c8f-8cc7-81f97dcdce45	Duration: 10974.37 ms	Billed Duration: 11571 ms	Memory Size: 1024 MB	Max Memory Used: 121 MB	Init Duration: 595.79 ms

# 4) Verify output

- `22:30:17`     size: 76,678b
- `22:30:17`     generated_at: 2026-05-05T22:30:16+00:00
- `22:30:17`     stats: {"n_total": 231, "n_raw_input": 48940, "n_pre_filter": 1800, "by_sector": {"Technology": 14, "Healthcare": 24, "Financial Services": 110, "Industrials": 25, "Consumer Cyclical": 11, "Consumer Defensive": 8, "Basic Materials": 12, "Unknown": 2, "Utilities": 8, "Communication Services": 6, "Energy": 4, "Real Estate": 7}, "by_mcap_bucket": {"mega (>$200B)": 5, "large ($10-200B)": 47, "mid ($2-10B)": 78, "small ($300M-2B)": 76, "micro (<$300M)": 25}, "statuses": {"ok": 231, "no_quote": 0, "below_mca
- `22:30:17`   
- `22:30:17`     ── top 10 by market cap ──
- `22:30:17`       AAPL    $4172.1B  Technology                 Apple Inc.
- `22:30:17`       ABBV    $364.6B   Healthcare                 AbbVie Inc.
- `22:30:17`       ABALX   $265.2B   Financial Services         American Funds American Balanced Fu
- `22:30:17`       ACGBY   $263.2B   Financial Services         Agricultural Bank of China Limited
- `22:30:17`       ACGBF   $259.0B   Financial Services         Agricultural Bank of China Limited
- `22:30:17`       ACSAY   $199.1B   Industrials                ACS, Actividades de Construcción y 
- `22:30:17`       ABLZF   $187.3B   Industrials                ABB Ltd
- `22:30:17`       ABBNY   $186.9B   Industrials                ABB Ltd
- `22:30:17`       ABT     $151.8B   Healthcare                 Abbott Laboratories
- `22:30:17`       AAIGF   $116.2B   Financial Services         AIA Group Limited