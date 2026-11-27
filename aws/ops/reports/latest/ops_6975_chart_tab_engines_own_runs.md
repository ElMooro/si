# 6975 - the five engines' own runs since the deploy of 14:59 UTC on 2026-11-26 (read-only), 2026-11-27T14:44+00:00

**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-11-27T14:44:29+00:00  

## Data

| carry_the_change | waiting_for_their_own_run | wrong |
|---|---|---|
| options, on-chain, insider trades, catalyst calendar, plumbing | none | none |

## Log
## 1. The documents

- `14:44:27` options | written 2026-11-27T14:40+00:00 (after the deploy) | CARRIES THE CHANGE | version 1.1.0, 155 of 155 names analysed in 7.1 s, not analysed: none
- `14:44:27` on-chain | written 2026-11-26T15:31+00:00 (after the deploy) | CARRIES THE CHANGE | CryptoQuant block present (status EOD, 171 metrics kept), Ethereum metrics 17, Ethereum errors ['coinmetrics_eth: HTTPError']
- `14:44:28` insider trades | written 2026-11-26T21:10+00:00 (after the deploy) | CARRIES THE CHANGE | version 3.1, buying total $834,258,004 over 720 buys, price check {"n_checked": 3870, "n_in_range": 3795, "n_off_tape": 34, "n_no_tape": 40, "n_set_aside": 1, "usd_set_aside_buys": 9919161280.9, "usd_set_aside_sells": 0.0, "tickers_on_the_book": 973}, set aside: LONA $9,919,161,281 (priced at 1,162.2 times the tape and worth more than the whole company (92,804% of its market value))
- `14:44:28` catalyst calendar | written 2026-11-27T12:00+00:00 (by the engine's own 12:00 UTC run) | version 2.0.0, 364 events, 243 ahead | ahead by type {"BANK_EARNINGS": 1, "EARNINGS": 148, "ECON": 79, "FOMC": 1, "IPO": 3, "MARKET_HOLIDAY": 5, "OPEX": 1, "QUARTER_END": 1, "REBALANCE": 1, "VIX_EXPIRY": 2, "WITCHING": 1}
- `14:44:28`       source AUCTION: 0 events | TreasuryDirect announced auctions
- `14:44:28`       source FDA: 0 events | FMP FDA calendar
- `14:44:28`       source LOCKUP: 0 events | data/lockup-expiration.json
- `14:44:28`       source REBALANCE: 0 events | data/russell-recon-frontrun.json
- `14:44:28`       economic events ahead: 79, 74 with a figure, 63 of those with a unit | units {"%": 38, "(none)": 11, "B": 3, "K": 19, "M": 1, "Points": 2}
- `14:44:28`       e.g. 2026-11-27 Retail Inventories Ex Autos MoM (Oct) | US · consensus 0.4% · previous 0.5% | unit '%'
- `14:44:28`       e.g. 2026-11-27 Goods Trade Balance (Oct) | US · consensus -133B · previous -135.1B | unit 'B'
- `14:44:28`       e.g. 2026-11-27 Wholesale Inventories MoM (Oct) | US · consensus 0.5% · previous 0.7% | unit '%'
- `14:44:28`       e.g. 2026-11-30 NBS Non Manufacturing PMI (Nov) | China · consensus 49.6 · previous 49.7 | unit None
- `14:44:28`       e.g. 2026-11-30 NBS Manufacturing PMI (Nov) | China · consensus 49.5 · previous 49.3 | unit None
- `14:44:28`       e.g. 2026-11-30 Chicago PMI (Nov) | US · consensus 55 · previous 55.3 | unit None
- `14:44:28` plumbing | written 2026-11-27T14:22+00:00 (after the deploy) | CARRIES THE CHANGE | header 58 indicators (55 with data) against 58 rows; base list 40
## 2. Each function since the deploy

- `14:44:28` options-analytics | 94 runs, 0 errors, 7.7 s a run on average, 9.7 s at most
- `14:44:28` onchain-ratios | 1 runs, 0 errors, 3.0 s a run on average, 3.0 s at most
- `14:44:29` insider-trades | 2 runs, 0 errors, 41.1 s a run on average, 49.0 s at most
- `14:44:29` catalyst-calendar | 1 runs, 0 errors, 1.8 s a run on average, 1.8 s at most
- `14:44:29` plumbing-aggregator | 1 runs, 0 errors, 44.5 s a run on average, 44.5 s at most
- `14:44:29` ✅ 5 carry the change, 0 wait for their own run
