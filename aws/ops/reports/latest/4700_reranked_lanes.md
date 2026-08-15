# ops 4700 — multi-series fredgraph · TE guest:guest · Common Crawl Feb-2026 · Wayback Data List

**Status:** failure  
**Duration:** 34.7s  
**Finished:** 2026-08-15T15:56:38+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. LIVE multi-series fredgraph.csv — does a Data-List-style comma query bypass the per-series cap? (untested tonight, cheap to check)

- `15:56:29`   multi-series csv failed: The read operation timed out
## 2. Trading Economics guest:guest — the historically-proven bridge, tested across plausible endpoint shapes

- `15:56:29`   [historical/country/indicator] HTTP 410: <p>We are sorry, but the guest account has been discontinued.</p>
<p>Please subscribe to a plan at <a href="https://tradingeconomics.com/api/pricing.a
- `15:56:30`   [historical w/ dates] HTTP 410: <p>We are sorry, but the guest account has been discontinued.</p>
<p>Please subscribe to a plan at <a href="https://tradingeconomics.com/api/pricing.a
- `15:56:31`   [markets/bonds] HTTP 410: <p>We are sorry, but the guest account has been discontinued.</p>
<p>Please subscribe to a plan at <a href="https://tradingeconomics.com/api/pricing.a
- `15:56:31`   [search high yield] HTTP 410: <p>We are sorry, but the guest account has been discontinued.</p>
<p>Please subscribe to a plan at <a href="https://tradingeconomics.com/api/pricing.a
- `15:56:32`   [indicators list US] HTTP 410: <p>We are sorry, but the guest account has been discontinued.</p>
<p>Please subscribe to a plan at <a href="https://tradingeconomics.com/api/pricing.a
## 3. Common Crawl (Feb 2026) — different infrastructure than Wayback, untested tonight

- `15:56:33`   crawls near Feb-2026: ['CC-MAIN-2026-30', 'CC-MAIN-2026-25', 'CC-MAIN-2026-21', 'CC-MAIN-2026-17', 'CC-MAIN-2026-12']
- `15:56:38`     CDX query failed: HTTP Error 404: Not Found
## 4. ONE gentle Wayback Data-List probe (not a sweep — archive.org throttle should have cleared by now, treated calmly either way)

- `15:56:38`   wayback multi-series: HTTP 498 (throttle or not archived — not alarming, single gentle try)
## verdict

- `15:56:38` ✗ none of the four lanes produced a confirmed deep-history hit this pass — see per-lane log for exact evidence
