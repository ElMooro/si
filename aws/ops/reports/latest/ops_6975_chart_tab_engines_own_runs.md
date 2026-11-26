# 6975 - the five engines' own runs since the deploy of 14:59 UTC on 2026-11-26 (read-only), 2026-11-26T15:33+00:00

**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-11-26T15:33:29+00:00  

## Data

| carry_the_change | waiting_for_their_own_run | wrong |
|---|---|---|
| options, on-chain | insider trades, catalyst calendar, plumbing | none |

## Log
## 1. The documents

- `15:33:27` options | written 2026-11-26T15:30+00:00 (after the deploy) | CARRIES THE CHANGE | version 1.1.0, 155 of 155 names analysed in 7.8 s, not analysed: none
- `15:33:28` on-chain | written 2026-11-26T15:31+00:00 (after the deploy) | CARRIES THE CHANGE | CryptoQuant block present (status EOD, 171 metrics kept), Ethereum metrics 17, Ethereum errors ['coinmetrics_eth: HTTPError']
- `15:33:28` insider trades | written 2026-11-25T21:10+00:00 (before the deploy: its own run has not come yet) | waiting | version 3.0, buying total $10,689,562,023 over 628 buys, price check null, set aside: none
- `15:33:28` catalyst calendar | written 2026-11-26T14:59+00:00 (by the publication from the runner (ops 6974); the engine's own run is 12:00 UTC) | version 2.0.0, 355 events, 230 ahead | ahead by type {"BANK_EARNINGS": 1, "EARNINGS": 137, "ECON": 79, "FOMC": 1, "MARKET_HOLIDAY": 6, "OPEX": 1, "QUARTER_END": 1, "REBALANCE": 1, "VIX_EXPIRY": 2, "WITCHING": 1}
- `15:33:28`       source AUCTION: 0 events | TreasuryDirect announced auctions
- `15:33:28`       source FDA: 0 events | FMP FDA calendar
- `15:33:28`       source IPO: 0 events | data/ipo-pipeline.json
- `15:33:28`       source LOCKUP: 0 events | data/lockup-expiration.json
- `15:33:28`       source REBALANCE: 0 events | data/russell-recon-frontrun.json
- `15:33:28` plumbing | written 2026-11-26T14:21+00:00 (before the deploy: its own run has not come yet) | waiting | header 40 indicators (55 with data) against 58 rows; base list None
## 2. Each function since the deploy

- `15:33:29` options-analytics | 7 runs, 0 errors, 7.4 s a run on average, 8.7 s at most
- `15:33:29` onchain-ratios | 1 runs, 0 errors, 3.0 s a run on average, 3.0 s at most
- `15:33:29` insider-trades | 0 runs, 0 errors, 0.0 s a run on average, 0.0 s at most
- `15:33:29` catalyst-calendar | 0 runs, 0 errors, 0.0 s a run on average, 0.0 s at most
- `15:33:29` plumbing-aggregator | 0 runs, 0 errors, 0.0 s a run on average, 0.0 s at most
- `15:33:29` ✅ 2 carry the change, 3 wait for their own run
