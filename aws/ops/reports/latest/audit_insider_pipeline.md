
# 1) Lambda config + schedule

- `17:42:59`     state: Active  mem=512MB  timeout=300s
- `17:42:59`     modified: 2026-04-27T21:56:19.000+0000
- `17:42:59`     env: ['S3_BUCKET', 'CLUSTER_WINDOW_DAYS', 'CLUSTER_MIN_INSIDERS', 'MIN_BUY_VALUE_USD', 'BIG_BUY_USD', 'SEC_USER_AGENT', 'WINDOW_DAYS', 'S3_KEY']
- `17:42:59`     schedule: justhodl-insider-trades-30min  expr=rate(30 minutes)  state=ENABLED

# 2) Current S3 output shape

- `17:42:59`     size: 16,613b  modified: 2026-05-05 17:33:56+00:00
- `17:42:59`     top-level keys: ['big_buys', 'cluster_window_days', 'clusters', 'generated_at', 'stats', 'thresholds', 'transactions', 'window_days']
- `17:42:59`     stats: {'total_buys': 33, 'total_value_usd': 39743581.36, 'unique_companies': 22, 'unique_insiders': 29, 'cluster_count': 1, 'big_buy_count': 8, 'fetch_errors': 92, 'fetch_duration_s': 5.8, 'diagnostics': {'filings_seen': 100, 'xml_fetched': 8, 'xml_parse_failed': 0, 'no_issuer_or_ticker': 0, 'no_nonderivative_table': 0, 'txn_seen': 12, 'skipped_not_buy_or_sell': 6, 'skipped_below_threshold': 0, 'buys_kept': 0, 'sells_kept': 6}, 'fetch_errlog': {'http_404_index': 63, 'http_404_html': 61, 'no_xml_in_listing': 15, 'http_429_index': 14, 'http_429_html': 16}, 'atom_feed_count': 100}
- `17:42:59`     clusters: 1
- `17:42:59`     ── Top 10 clusters by total_value ──
- `17:42:59`       N/A      4-insiders $    24,373,263 4-txns  John Hancock GA Senior Loan Trust
- `17:42:59`   
- `17:42:59`     ── Sample cluster fields ──
- `17:42:59`       ticker: N/A
- `17:42:59`       company: John Hancock GA Senior Loan Trust
- `17:42:59`       cik: 0001742951
- `17:42:59`       insider_count: 4
- `17:42:59`       transactions: 4
- `17:42:59`       total_shares: 1443386
- `17:42:59`       total_value: 24373262.53
- `17:42:59`       avg_price: 16.89
- `17:42:59`       first_filing: 2026-04-30T13:21:20-04:00
- `17:42:59`       last_filing: 2026-05-04T10:42:54-04:00
- `17:42:59`       insiders: [4 items]
- `17:42:59`     big_buys (>$1M single tx): 8
- `17:42:59`     ── Top 8 big single buys ──
- `17:42:59`       N/A      Manufacturers Life Reinsurance Ltd  $  15,994,407  10% Owner
- `17:42:59`       N/A      Manufacturers Life Insurance Co (Bermuda Branch)  $   3,998,602  10% Owner
- `17:42:59`       AB-LEND  PM Alpha DAC                    $   3,661,041  10% Owner
- `17:42:59`       AB-LEND  PM Alpha DAC                    $   2,618,382  10% Owner
- `17:42:59`       CSGP     FLORANCE ANDREW C               $   2,403,166  Director, President and CEO
- `17:42:59`       N/A      UNIVERSITY OF TEXAS/TEXAS AM INVESTMENT MANAGEMENT CO  $   2,380,952  10% Owner
- `17:42:59`       N/A      Manulife (Singapore) Pte. Ltd.  $   1,999,301  10% Owner
- `17:42:59`       IPX      Hannigan Todd                   $   1,091,766  Director, Executive Chairman
- `17:42:59`     sector_heat: 0 sectors

# 3) Is anything consuming this data?

- `17:42:59`     HTML pages referencing insider-trades.json: ['insiders.html', 'research.html', 'signals.html', 'today.html', 'news.html']
- `17:42:59`     Lambdas referencing insider-trades.json: ['justhodl-insider-trades', 'justhodl-backtest-harness', 'justhodl-asymmetric-scorer', 'justhodl-ai-brief']

# 4) What's the universe of cluster opportunities?

- `17:42:59`     cluster size distribution: {4: 1}
- `17:42:59`     cluster value distribution: {'<100k': 0, '100k-500k': 0, '500k-1M': 0, '1M-5M': 0, '5M+': 1}
- `17:42:59`   
- `17:42:59`     STRONG cluster signals (3+ insiders AND >$500k): 1
- `17:42:59`       N/A      $  24,373,263 4-insiders roles=['10% Owner']