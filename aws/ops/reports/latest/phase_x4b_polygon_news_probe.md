
# 1) Polygon /v2/reference/news (no filter)

- `09:07:30`       ✓ all news status=200
- `09:07:30`         keys=['results', 'status', 'request_id', 'count', 'next_url']
- `09:07:30`         n_results: 10
- `09:07:30`         first item keys: ['id', 'publisher', 'title', 'author', 'published_utc', 'article_url', 'tickers', 'image_url', 'description', 'keywords', 'insights']
- `09:07:30`         title: PONY AI Inc. to Report First Quarter 2026 Financial Results on May 26, 2026
- `09:07:30`         published_utc: 2026-05-06T09:00:00Z
- `09:07:30`         tickers: ['PONY']
- `09:07:30`         has pagination cursor

# 2) Polygon news filtered by ticker

- `09:07:31`       ✓ AAPL news status=200
- `09:07:31`         keys=['results', 'status', 'request_id', 'count', 'next_url']
- `09:07:31`         n_results: 10
- `09:07:31`         first item keys: ['id', 'publisher', 'title', 'author', 'published_utc', 'article_url', 'tickers', 'image_url', 'description', 'keywords', 'insights']
- `09:07:31`         title: Apple, Amazon, TSMC Top List Of Earnings Triple Plays As Beat-And-Raise Stocks Surge
- `09:07:31`         published_utc: 2026-05-05T19:05:11Z
- `09:07:31`         tickers: ['AAPL', 'AMZN', 'TSM', 'NVDA', 'AMD']
- `09:07:31`         has pagination cursor

# 3) Polygon news with date filter

- `09:07:31`       ✓ news last 7 days status=200
- `09:07:31`         keys=['results', 'status', 'request_id', 'count', 'next_url']
- `09:07:31`         n_results: 50
- `09:07:31`         first item keys: ['id', 'publisher', 'title', 'author', 'published_utc', 'article_url', 'tickers', 'image_url', 'description', 'keywords', 'insights']
- `09:07:31`         title: PONY AI Inc. to Report First Quarter 2026 Financial Results on May 26, 2026
- `09:07:31`         published_utc: 2026-05-06T09:00:00Z
- `09:07:31`         tickers: ['PONY']
- `09:07:31`         has pagination cursor

# 4) Polygon news bulk (200 limit)

- `09:07:31`       ✓ news bulk 200 status=200
- `09:07:31`         keys=['results', 'status', 'request_id', 'count', 'next_url']
- `09:07:31`         n_results: 200
- `09:07:31`         first item keys: ['id', 'publisher', 'title', 'author', 'published_utc', 'article_url', 'tickers', 'image_url', 'description', 'keywords', 'insights']
- `09:07:31`         title: PONY AI Inc. to Report First Quarter 2026 Financial Results on May 26, 2026
- `09:07:31`         published_utc: 2026-05-06T09:00:00Z
- `09:07:31`         tickers: ['PONY']
- `09:07:31`         has pagination cursor

# 5) Polygon news ticker + date

- `09:07:32`       ✓ NVDA news last 7d status=200
- `09:07:32`         keys=['results', 'status', 'request_id', 'count', 'next_url']
- `09:07:32`         n_results: 50
- `09:07:32`         first item keys: ['id', 'publisher', 'title', 'author', 'published_utc', 'article_url', 'tickers', 'image_url', 'description', 'keywords', 'insights']
- `09:07:32`         title: Should You Buy Nvidia Stock Before May 20?
- `09:07:32`         published_utc: 2026-05-06T08:15:00Z
- `09:07:32`         tickers: ['NVDA']
- `09:07:32`         has pagination cursor