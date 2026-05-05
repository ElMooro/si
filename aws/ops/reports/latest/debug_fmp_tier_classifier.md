
# 1) FMP — fetch /profile/AAPL

- `14:26:18`   GET /profile/AAPL
- `14:26:18`     → HTTPError 403: {
  "Error Message": "Legacy Endpoint : Due to Legacy endpoints being no longer supported - This endpoint is only available for legacy users who have valid subscriptions prior August 31, 2025. Please visit our documentation page https://site.financialmodelingprep.com/developer/docs for our current A

# 2) FMP — fetch /key-metrics-ttm/AAPL

- `14:26:18`   GET /key-metrics-ttm/AAPL?limit=1
- `14:26:18`     → HTTPError 403: {
  "Error Message": "Legacy Endpoint : Due to Legacy endpoints being no longer supported - This endpoint is only available for legacy users who have valid subscriptions prior August 31, 2025. Please visit our documentation page https://site.financialmodelingprep.com/developer/docs for our current A

# 3) FMP — fetch /ratios-ttm/AAPL

- `14:26:18`   GET /ratios-ttm/AAPL
- `14:26:19`     → HTTPError 403: {
  "Error Message": "Legacy Endpoint : Due to Legacy endpoints being no longer supported - This endpoint is only available for legacy users who have valid subscriptions prior August 31, 2025. Please visit our documentation page https://site.financialmodelingprep.com/developer/docs for our current A

# 4) FMP — try /profile/MU (memory leader)

- `14:26:19`   GET /profile/MU
- `14:26:19`     → HTTPError 403: {
  "Error Message": "Legacy Endpoint : Due to Legacy endpoints being no longer supported - This endpoint is only available for legacy users who have valid subscriptions prior August 31, 2025. Please visit our documentation page https://site.financialmodelingprep.com/developer/docs for our current A

# 5) FMP — try v3 /quote/MU

- `14:26:19`   GET /quote/MU
- `14:26:19`     → HTTPError 403: {
  "Error Message": "Legacy Endpoint : Due to Legacy endpoints being no longer supported - This endpoint is only available for legacy users who have valid subscriptions prior August 31, 2025. Please visit our documentation page https://site.financialmodelingprep.com/developer/docs for our current A

# 6) Try fetching through the deployed Lambda — /tmp/AAPL invoke

- `14:26:20`   Lambda env vars: ['FMP_KEY']
- `14:26:20`     FMP_KEY length: 32
- `14:26:20`     FMP_KEY starts: wwVpi37S...

# 7) Inspect tier-classifier source — fetch_fundamentals function

- `14:26:20`   Lambda code url retrieved (1223 chars)
- `14:26:20`   Lambda code zip size: 20,813b
- `14:26:20`   ── deployed fetch_fundamentals ──
- `14:26:20`     def fetch_fundamentals(ticker):
- `14:26:20`         """
- `14:26:20`         Pull profile + key-metrics-ttm from FMP. Returns dict or None.
- `14:26:20`         """
- `14:26:20`         with _CACHE_LOCK:
- `14:26:20`             if ticker in _FUNDAMENTAL_CACHE:
- `14:26:20`                 return _FUNDAMENTAL_CACHE[ticker]
- `14:26:20`     
- `14:26:20`         profile = fmp_get(f"/profile/{ticker}")
- `14:26:20`         if not profile or not isinstance(profile, list) or not profile:
- `14:26:20`             result = None
- `14:26:20`         else:
- `14:26:20`             p = profile[0]
- `14:26:20`             # key-metrics-ttm has the trailing-twelve-month ratios
- `14:26:20`             ktm = fmp_get(f"/key-metrics-ttm/{ticker}", params={"limit": "1"})
- `14:26:20`             kt = ktm[0] if (ktm and isinstance(ktm, list) and ktm) else {}
- `14:26:20`     
- `14:26:20`             # ratios-ttm has additional ratios
- `14:26:20`             rtm = fmp_get(f"/ratios-ttm/{ticker}")
- `14:26:20`             rt = rtm[0] if (rtm and isinstance(rtm, list) and rtm) else {}
- `14:26:20`     
- `14:26:20`             market_cap = p.get("mktCap")
- `14:26:20`             revenue_ttm = kt.get("revenuePerShareTTM")
- `14:26:20`             shares = (market_cap / p.get("price")) if (market_cap and p.get("price")) else None
- `14:26:20`             if revenue_ttm is not None and shares:
- `14:26:20`                 revenue_ttm_total = revenue_ttm * shares
- `14:26:20`             else:
- `14:26:20`                 revenue_ttm_total = None
- `14:26:20`     
- `14:26:20`             result = {
- `14:26:20`                 "ticker": ticker,
- `14:26:20`                 "name": p.get("companyName"),
- `14:26:20`                 "sector": p.get("sector"),
- `14:26:20`                 "industry": p.get("industry"),
- `14:26:20`                 "exchange": p.get("exchangeShortName"),
- `14:26:20`                 "country": p.get("country"),
- `14:26:20`                 "currency": p.get("currency"),
- `14:26:20`                 "price": p.get("price"),
- `14:26:20`                 "market_cap": market_cap,
- `14:26:20`                 "shares_