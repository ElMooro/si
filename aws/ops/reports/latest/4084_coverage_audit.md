# ops 4084 — AUDIT: is every imported ticker FETCHABLE?

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-07-29T06:31:23+00:00  

## Data

| fetchable | macro_attributed | macro_fetchable | macro_gap | namespaces | unique_tickers | unrouted | vault_covered | vault_live | vault_rows | watchlists |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 172 | 10319 |  |  |  |  | 491 |
|  |  |  |  |  |  |  | 535 | 480 | 591 |  |
| 4504 |  |  | 4369 |  |  | 1446 |  |  |  |  |
|  | 239 | 234 |  |  |  |  |  |  |  |  |

## Log
## A. imported ticker universe

- `06:31:23`   watchlists: 491   unique tickers: 10319
- `06:31:23`        500  71699273
- `06:31:23`        500  82604570
- `06:31:23`        500  Black Swan Event
- `06:31:23`        500  Bottom Indicators
- `06:31:23`        500  FTSE
- `06:31:23`        500  Red list
- `06:31:23`        437  Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.
- `06:31:23`        434  TOP
- `06:31:23`        433  Watchlist
- `06:31:23`        373  Top Indicators
- `06:31:23`        359  Global Liquidity Trend Reversal
- `06:31:23`        327  Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.
- `06:31:23`        324  68114374
- `06:31:23`        273  Brent Johnson Portfolio: THE SHORTTERM SWINGS MIGHT GRAB HEADLINES BUT THE LONG TERM TRAJECTORY IS UMISTAKABLE.
- `06:31:23`   distinct namespaces: 172
## B. namespace breakdown (top 25)

- `06:31:23`     3317  ECONOMICS
- `06:31:23`     1535  NASDAQ
- `06:31:23`      765  FRED
- `06:31:23`      653  AMEX
- `06:31:23`      455  FTSE
- `06:31:23`      371  TVC
- `06:31:23`      330  INDEX
- `06:31:23`      277  NYSE
- `06:31:23`      229  COT3
- `06:31:23`      169  CBOE
- `06:31:23`      151  FX_IDC
- `06:31:23`      150  INTOTHEBLOCK
- `06:31:23`      143  USI
- `06:31:23`      111  COT
- `06:31:23`      107  SGX
- `06:31:23`       87  DJ
- `06:31:23`       65  LSE
- `06:31:23`       62  GLASSNODE
- `06:31:23`       61  SSE
- `06:31:23`       59  EURONEXT
- `06:31:23`       58  SWB
- `06:31:23`       52  HSI
- `06:31:23`       50  ICEEUR
- `06:31:23`       42  CME
- `06:31:23`       42  EUREX
## C. vault: what is fetchable RIGHT NOW

- `06:31:23`   vault rows 591   LIVE 480
- `06:31:23`   adapters: fmp 179, (none) 121, fred 107, yahoo 59, mofjp 3, ecb 2, norges 2, bcb 2, bcrp 2, poly 1, ust 1, boj 1
- `06:31:23`   imported tickers with a LIVE vault row: 535
## D. route classification for every imported ticker

- `06:31:23`     4369  macro-needs-mapping
- `06:31:23`     2959  equity-api
- `06:31:23`     1446  UNROUTED
- `06:31:23`      957  fx-index-api
- `06:31:23`      535  vault-live
- `06:31:23`       53  crypto-api
- `06:31:23`   → plausibly fetchable today : 4504 (43.6%)
- `06:31:23`   → macro needing a series map: 4369
- `06:31:23`   → no route at all           : 1446
## E. the UNROUTED tail — which namespaces?

- `06:31:23`      455  FTSE
- `06:31:23`      151  CBOE
- `06:31:23`      150  INTOTHEBLOCK
- `06:31:23`       62  GLASSNODE
- `06:31:23`       58  SWB
- `06:31:23`       52  HSI
- `06:31:23`       40  CBOEEU
- `06:31:23`       33  TRADEGATE
- `06:31:23`       29  CSEMA
- `06:31:23`       20  OMXNORDIC
- `06:31:23`       17  BER
- `06:31:23`       16  CME_MINI
- `06:31:23`       16  GETTEX
- `06:31:23`       14  DFM
- `06:31:23`       14  1-TVC
- `06:31:23`       13  ADX
- `06:31:23`       12  DJCFD
- `06:31:23`       12  SPARKS
- `06:31:23`       12  MULTPL
- `06:31:23`       12  TSE
- `06:31:23`   sample: CBOE:BMNU, CBOE:SBTU, OKEX:CVXUSDT, DJCFD:DE30, OMXBALTIC:B55PI, CBOE:FDEM, TRADEGATE:FJ2B, TRADEGATE:FJ25, NEO:FGRO, SWB:FJ2B, SWB:FJ2F, SWB:FJ25
## F. macro: attribution ledger vs fetchability

- `06:31:23`   macro symbols attributed so far: 239
- `06:31:23`   ...of which carry a FRED series id (=> FETCHABLE): 234
- `06:31:23`   NOTE: a FRED-routed macro symbol is not just labelled, it is
- `06:31:23`   pullable — the series id is the fetch key. That is the bridge
- `06:31:23`   from 'who publishes this' to 'my system can chart this'.
## VERDICT — where the real work is

- `06:31:23`   universe 10319 · fetchable-ish 4504 · macro-gap 4369 · unrouted 1446
- `06:31:23`   AUDIT ONLY — no engine code written.
