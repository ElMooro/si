# 6973 - dry run of the engine changes for the chart tabs (nothing written), 2026-11-26T14:49+00:00

**Status:** success  
**Duration:** 42.2s  
**Finished:** 2026-11-26T14:50:17+00:00  

## Data

| failed_checks |
|---|
| 0 |

## Log
## 1. Catalyst calendar 2.0

- `14:49:40` built in 3.8 s | events 355 (ahead 230, past 125) against the live calendar's 125
- `14:49:40` by type, ahead: {"BANK_EARNINGS": 1, "EARNINGS": 137, "ECON": 79, "FOMC": 1, "MARKET_HOLIDAY": 6, "OPEX": 1, "QUARTER_END": 1, "REBALANCE": 1, "VIX_EXPIRY": 2, "WITCHING": 1}
- `14:49:40` by type, live calendar: {"EARNINGS_ACTUAL": 113, "EARNINGS": 8, "FOMC": 1, "REBALANCE": 1, "WITCHING": 1, "BANK_EARNINGS": 1}
- `14:49:40`    source FOMC               1 events | Federal Reserve schedule (in the engine)
- `14:49:40`    source AUCTION            0 events | TreasuryDirect announced auctions
- `14:49:40`    source WITCHING           1 events | calendar rule
- `14:49:40`    source REBALANCE          1 events | calendar rule
- `14:49:40`    source BANK_EARNINGS      1 events | calendar rule (usual date, not announced)
- `14:49:40`    source EARNINGS           8 events | data/earnings-tracker.json
- `14:49:40`    source EARNINGS_ACTUAL  113 events | data/earnings-tracker.json
- `14:49:40`    source FDA                0 events | FMP FDA calendar
- `14:49:40`    source GOV_CONTRACT      12 events | USAspending.gov award search
- `14:49:40`    source ECON              80 events | data/econ-calendar.json | 3.8 h old
- `14:49:40`    source EARNINGS         129 events | data/finviz-earnings-calendar.json | 16.8 h old
- `14:49:40`    source IPO                0 events | data/ipo-pipeline.json | 16.3 h old
- `14:49:40`    source LOCKUP             0 events | data/lockup-expiration.json | 16.8 h old
- `14:49:40`    source QUARTER_END        1 events | data/rebalance-radar.json | 16.2 h old
- `14:49:40`    source REBALANCE          0 events | data/russell-recon-frontrun.json | 2.3 h old
- `14:49:40`    source MARKET_HOLIDAY     6 events | data/warm/polygon-bank/market-holidays/daily/2026-11-24.json.gz | 49.9 h old
- `14:49:40`    source OPEX               1 events | calendar rule (third Friday)
- `14:49:40`    source VIX_EXPIRY         2 events | calendar rule (30 days before the next third Friday)
- `14:49:40` PASS  no event is listed twice (355 events)
- `14:49:40` PASS  every event has a type, a title and an impact
- `14:49:40` PASS  every event is dated and the list is in date order
- `14:49:40` PASS  every event ahead on the live calendar is still there (0 missing)
- `14:49:40` PASS  economic releases are on the calendar (79)
- `14:49:40` PASS  at least three times as many events ahead as the live calendar (230 against 12)
- `14:49:40` FOMC rows: [["2026-12-16", "FOMC Statement"]]
- `14:49:40` market holidays: [["2026-11-26", "Thanksgiving (market closed)", null], ["2026-11-27", "Thanksgiving (early close)", "18:00"], ["2026-12-24", "Christmas (early close)", "18:00"], ["2026-12-25", "Christmas (market closed)", null], ["2027-01-01", "New Years Day (market closed)", null], ["2027-01-18", "Martin Luther King, Jr. Day (market closed)", null]]
- `14:49:40` contract awards: 12 | date taken from: ["Last Modified Date"] | sample [["2026-10-30", "HON", "$18055.5M federal award \u00b7 Department of Energy"], ["2026-11-02", "LDOS", "$3279.5M federal award \u00b7 National Science Foundation"], ["2026-11-02", "LDOS", "$1151.6M federal award \u00b7 General Services Administrat"]]
- `14:49:40`    ECON          79 ahead | first: [["2026-11-26", "BoJ Tamura Speech", "Japan", "MEDIUM"], ["2026-11-27", "Housing Starts YoY (Oct)", "Japan · consensus 2% · previous -2.7%", "MEDIUM"]]
- `14:49:40`    EARNINGS     137 ahead | first: [["2026-11-27", "TRX earnings", "TRX Gold Corp · after the close · $0.2B", "LOW"], ["2026-11-27", "XYF earnings", "X Financial ADR · before the open · $0.2B", "LOW"]]
- `14:49:40`    IPO            0 ahead | first: []
- `14:49:40`    LOCKUP         0 ahead | first: []
- `14:49:40`    QUARTER_END    1 ahead | first: [["2026-12-31", "Quarter-end", "Rebalance window T-5..T+3 business days around quarter-end", "MEDIUM"]]
- `14:49:40`    OPEX           1 ahead | first: [["2027-01-15", "Monthly options expiration", "Stock and index options expire", "MEDIUM"]]
- `14:49:40`    VIX_EXPIRY     2 ahead | first: [["2026-12-16", "VIX expiration", "VIX futures and options settle at the open", "LOW"], ["2027-01-20", "VIX expiration", "VIX futures and options settle at the open", "LOW"]]
- `14:49:40` document size 153,448 bytes against 68,407 live
## 2. Options: 155 names

- `14:49:40` nearest monthly expiry 2026-12-18 | 97 names added to the first 58
- `14:50:02` PASS  the added names have listed options at the provider (0 without: none)
- `14:50:15` whole run in 13.2 s | universe 155, analysed 155, not analysed: none | writes captured, not sent: data/options-analytics-iv-history.json (181,097 bytes), data/options-analytics.json (150,161 bytes)
- `14:50:15` distribution {"short_gamma": 89, "long_gamma": 66, "squeeze_setups": 33, "downside_hedging": 24} | status 200
- `14:50:15` PASS  at least 140 of the 155 names analysed (155)
- `14:50:15` PASS  the run stays light for the five-minute lane (13.2 s)
- `14:50:15` added names on the board: 97 | sample [["TSM", "GAMMA_SQUEEZE_SETUP", "SHORT_GAMMA", -45.0], ["EWZ", "NEUTRAL", "LONG_GAMMA", 4.56], ["GLD", "GAMMA_SQUEEZE_SETUP", "SHORT_GAMMA", -243.95], ["GDX", "GAMMA_SQUEEZE_SETUP", "SHORT_GAMMA", -41.83], ["JNJ", "NEUTRAL", "LONG_GAMMA", 28.05], ["NIO", "GAMMA_SQUEEZE_SETUP", "SHORT_GAMMA", -0.67]]
- `14:50:15` PASS  every added name on the board has a spot price and a signal
## 3. Insider trades: price check against the tape

- `14:50:17` screened 3628 rows in 1.2 s | {"n_checked": 3628, "n_in_range": 3561, "n_off_tape": 31, "n_no_tape": 35, "n_set_aside": 1, "usd_set_aside_buys": 9919161280.9, "usd_set_aside_sells": 0.0, "tickers_on_the_book": 912}
- `14:50:17`    set aside: LONA buy 3,149,470 shares at 3149.47 = $9,919,161,281 | tape 2.71 | priced at 1,162.2 times the tape and worth more than the whole company (92,804% of its market value)
- `14:50:17` buys counted: 628 -> 627 | total value $10,689,562,023 -> $770,400,742
- `14:50:17` PASS  the factor book answered for the tickers (912)
- `14:50:17` PASS  the LONA line is set aside
- `14:50:17` PASS  every row is either counted or set aside, and very few are set aside (1 of 3628)
- `14:50:17` PASS  the buying total no longer carries the $9.9 billion line
## 4. On-chain: the join with the kept CryptoQuant document

- `14:50:17` live document had the join: [False] | after: status EOD, written 2026-11-24T21:05, 171 metrics kept
- `14:50:17` Bitcoin core set: {"btc_whale_ratio": 0.243772, "btc_mvrv": 1.151253, "btc_sopr": 0.986539, "btc_exchange_netflow": 143.263854, "btc_mpi": 1.1135, "btc_nupl": 0.131381, "btc_puell": 0.956718, "btc_nvt_golden": -1.298372, "btc_realized_price": 53209.495378, "btc_ssr": 12.824014}
- `14:50:17` Ethereum metrics joined: 17 | {"eth_exchange_netflow": 47030.985449, "eth_exchange_inflow": 812079.56829, "eth_inflow_top10": 239720.603192, "eth_inflow_mean": 32.481883, "eth_exchange_outflow": 765048.58284, "eth_outflow_top10": 156389.584249, "eth_outflow_mean": 4.681573, "eth_exchange_reserve": 14719374.917086}
- `14:50:17` PASS  the Bitcoin core set is joined again
- `14:50:17` PASS  Ethereum has its metrics (17)
## 5. Plumbing: the indicator count

- `14:50:17` live header says 40 indicators, 55 with data | the document carries 58 rows, its layers add to 58
- `14:50:17` PASS  the rows and the layers agree on the count the new header will print (58)
- `14:50:17` ✅ all checks passed; nothing written, nothing invoked
