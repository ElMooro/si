# ops 6657 - FMP key health: are we rate-limited right now? (read-only)

**Status:** success  
**Duration:** 87.9s  
**Finished:** 2026-10-26T01:54:35+00:00  

## Data

| http429 | ok | other | step |
|---|---|---|---|
| 12 | 0 | 0 | fmp key health |

## Log
- `01:53:07` key: SSM /justhodl/fmp/api-key, 32 chars, sha256 prefix f2356cd4 (value never printed)
## 1. three spaced calls now

- `01:53:07` 01:53:07 profile -> {"status": 429, "ms": 87, "headers": {"server": "nginx", "date": "Mon, 26 Oct 2026 01:53:07 GMT"}, "body": "{\n  \"Error Message\": \"Bandwidth Limit Reach . Please upgrade your plan or visit our documentation for more details at https://site.financialmodelingprep.com/\"\n}"}
- `01:53:10` 01:53:10 quote -> {"status": 429, "ms": 62, "headers": {"server": "nginx", "date": "Mon, 26 Oct 2026 01:53:10 GMT"}, "body": "{\n  \"Error Message\": \"Bandwidth Limit Reach . Please upgrade your plan or visit our documentation for more details at https://site.financialmodelingprep.com/\"\n}"}
- `01:53:14` 01:53:14 historical-price-eod/full -> {"status": 429, "ms": 89, "headers": {"server": "nginx", "date": "Mon, 26 Oct 2026 01:53:14 GMT"}, "body": "{\n  \"Error Message\": \"Bandwidth Limit Reach . Please upgrade your plan or visit our documentation for more details at https://site.financialmodelingprep.com/\"\n}"}
## 2. after 65 s (a per-minute window must have rolled)

- `01:54:22` 01:54:22 profile MSFT -> {"status": 429, "ms": 358, "headers": {"server": "nginx", "date": "Mon, 26 Oct 2026 01:54:22 GMT"}, "body": "{\n  \"Error Message\": \"Bandwidth Limit Reach . Please upgrade your plan or visit our documentation for more details at https://site.financialmodelingprep.com/\"\n}"}
## 3. slow steady cadence: 12 calls at 1 call/s

- `01:54:35` statuses: [429, 429, 429, 429, 429, 429, 429, 429, 429, 429, 429, 429]
## 4. who else calls FMP in the 22:00-02:00 UTC window (schedule manifest, by expression)

- `01:54:35` 152 enabled triggers between 22:00 and 02:00 UTC: 00:00 justhodl-multi-tf-convergence, 00:00 justhodl-post-earnings-mean-rev, 00:10 justhodl-signal-fabric, 00:15 justhodl-smart-beta, 00:30 justhodl-catalyst-skew-premove, 00:30 justhodl-desk-allocator, 00:30 justhodl-equity-confluence, 00:35 justhodl-stock-xray, 00:45 justhodl-crypto-ma200, 00:45 justhodl-eva-spread, 01:00 justhodl-finra-short, 01:00 justhodl-firm-book, 01:00 justhodl-miss-detector, 01:07 justhodl-gdelt-full, 01:15 justhodl-crypto-dvol, 01:15 justhodl-crypto-scorecard, 01:15 justhodl-feed-heartbeat, 01:20 justhodl-crypto-options-surface, 01:30 justhodl-risk-monitor, 01:35 justhodl-valuations-agent, 01:40 justhodl-brief-compiler, 01:45 justhodl-crypto-stablecoin-peg, 01:50 justhodl-brief-compiler, 22:00 justhodl-crypto-liquidity, 22:00 justhodl-distribution-composite, 22:00 justhodl-dix, 22:00 justhodl-etf-fund-flows, 22:00 justhodl-finviz-universe, 22:00 justhodl-fx-decomposition, 22:00 justhodl-massive-signals, 22:00 justhodl-meta-improver, 22:00 justhodl-scheduler, 22:01 justhodl-backtest-harness, 22:01 justhodl-buzz-velocity, 22:01 justhodl-cot-tracker, 22:01 justhodl-proven-portfolio, 22:02 justhodl-52wk-quality-breakout, 22:02 justhodl-breadth-divergence, 22:02 justhodl-earnings-iv-crush, 22:02 justhodl-gf-value, 22:03 justhodl-liquidity-profile, 22:03 justhodl-lockup-expiration, 22:03 justhodl-sp500, 22:03 justhodl-vvix-vov-regime, 22:05 justhodl-calibrator, 22:05 justhodl-deal-scanner, 22:05 justhodl-sector-emergence, 22:06 justhodl-supply-chain-graph, 22:07 justhodl-signal-logger, 22:07 justhodl-tradingview, 22:08 justhodl-risk-regime, 22:10 justhodl-inverse-harvester, 22:11 justhodl-readthrough, 22:13 justhodl-provider-catalog, 22:13 justhodl-rotation-dashboard, 22:15 justhodl-finviz-groups, 22:15 justhodl-global-macro, 22:15 justhodl-macro-regime, 22:16 justhodl-catalyst-chain, 22:20 justhodl-brief-compiler, 22:20 justhodl-self-critique, 22:20 justhodl-usd-funding, 22:25 justhodl-implied-prob, 22:30 justhodl-backtest-engine:live, 22:30 justhodl-capital-flow-radar, 22:30 justhodl-cascade-validator, 22:30 justhodl-earnings-confluence, 22:30 justhodl-insider-aggregate, 22:30 justhodl-leading-markets, 22:30 justhodl-near-miss-monitor, 22:30 justhodl-wl-engines, 22:31 justhodl-cef-discount, 22:31 justhodl-failed-pattern-reversal, 22:31 justhodl-ipo-pipeline, 22:31 justhodl-skew-tail-hedging, 22:32 justhodl-event-study, 22:35 justhodl-credit-before-equity, 22:35 justhodl-historical-analogs, 22:35 justhodl-rebalance-radar, 22:38 justhodl-fmp-ratios, 22:40 justhodl-fmp-walker, 22:40 justhodl-positioning-analog, 22:40 justhodl-proven-alpha, 22:41 justhodl-polygon-bank, 22:45 justhodl-cross-asset-rv, 22:45 justhodl-etf-constituents, 22:45 justhodl-gold-equity-rotation, 22:45 justhodl-magic-formula, 22:45 justhodl-scarcity-radar, 22:45 justhodl-thesis-engine, 22:48 justhodl-wl-fusion, 22:50 justhodl-asset-compass, 22:50 justhodl-correlation-break-trade-router, 22:52 justhodl-tv-bars, 22:53 justhodl-macro-confluence, 22:53 justhodl
- `01:54:35` justhodl-fmp-walker last run: started None calls 3024 http {"http429": 756}
- `01:54:35` justhodl-fmp-bank last run: started None calls 13 http {"http429": 13, "err:budget": 1}
- `01:54:35` ✅ ops 6657 complete (read-only)
