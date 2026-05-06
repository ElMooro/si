
# 1) Probe Polygon /v3/snapshot/options/{underlying} — real-time chain

- `08:44:40`       ❌ AAPL chain snapshot HTTP 403: {"status":"NOT_AUTHORIZED","request_id":"12a3289bf1a7e83d6fa1c4e1b683f6e5","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}

# 2) Probe /v3/reference/options/contracts — contract list (need for sweep tracking)

- `08:44:40`       ✓ AAPL contracts list status=200
- `08:44:40`         keys=['results', 'status', 'request_id', 'next_url']
- `08:44:40`         sample={"results": "list[5]", "status": "OK", "request_id": "aeea3c5a93883e1d8ae3f763cfc84ddb", "next_url": "https://api.polygon.io/v3/reference/options/contracts?cursor"}
- `08:44:40`         results[0] keys: ['cfi', 'contract_type', 'exercise_style', 'expiration_date', 'primary_exchange', 'shares_per_contract', 'strike_price', 'ticker', 'underlying_ticker']

# 3) Probe /v2/aggs/ticker — options bar history

- `08:44:41`       ✓ AAPL option daily bars status=200
- `08:44:41`         keys=['ticker', 'queryCount', 'resultsCount', 'adjusted', 'status', 'request_id']
- `08:44:41`         sample={"ticker": "O:AAPL250620C00200000", "queryCount": "0", "resultsCount": "0", "adjusted": "True", "status": "DELAYED"}

# 4) Probe /v3/trades — options trades for sweep detection

- `08:44:41`       ❌ AAPL option trades HTTP 403: {"status":"NOT_AUTHORIZED","request_id":"f61a1d346a247b16bd6f4da7ab12b508","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}

# 5) Probe /v2/snapshot/options — alternative endpoint

- `08:44:41`       ❌ AAPL option v2 snapshot HTTP 404: 404 page not found

# 6) Probe /v1/last/options — latest quote

- `08:44:41`       ❌ AAPL latest NBBO HTTP 404: 404 page not found

# 7) Probe stable equity quote (baseline — should always work)

- `08:44:41`       ❌ AAPL equity snapshot HTTP 404: 404 page not found

# 8) Test FINRA short interest data (free, no key needed)

- `08:44:42`       ✓ FINRA daily short volume 20260504 raw: Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20260504|A|299392.172804|2112|743135.775886|B,Q,N
20260504|AA|780491.150794|643|1852775.004909|B,Q,N
20260504|AAA|912|0|5212.208544|Q
20260504|AAAA|32|0|1576.493017|Q
20260504|AAAC|1|0|1.029900|Q
20260504|AAAU|252716.836011|6404|450745.207470|B,Q,N
20260504|AACG|1304.058977|0|2536.746306|Q
20260504|AACIW|926|0|1400|Q
20260504|AA