# Master Data Source Verification — every source ever mentioned

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-04-24T22:53:06+00:00  

## Data

| active | degraded | inactive | total |
|---|---|---|---|
| 19 | 3 | 7 | 26 |

## Log
## 1. Data Source Matrix — production code references

- `22:53:06`   ❌ MISSING    FRED                      → no Lambda code references fred.stlouisfed.org
- `22:53:06`              purpose: 233 economic series — rates, CPI, balance sheet, GDP, employment
- `22:53:06`   ✅ ACTIVE      ECB SDMX                  → justhodl-ecb-proxy(1x), justhodl-khalid-metrics(5x), justhodl-daily-report-v3(1x)
- `22:53:06`              purpose: CISS systemic risk, ECB rates, eurozone subindices
- `22:53:06`   ✅ ACTIVE      NY Fed                    → justhodl-repo-monitor(3x)
- `22:53:06`              purpose: Reverse repo, RRP, SOFR, treasury operations
- `22:53:06`   ✅ ACTIVE      US Treasury               → justhodl-treasury-proxy(1x)
- `22:53:06`              purpose: TGA balance, debt issuance, auction results
- `22:53:06`   ❌ MISSING    BEA                       → no Lambda code references bea.gov
- `22:53:06`              purpose: GDP, personal income/spending
- `22:53:06`   ❌ MISSING    BLS                       → no Lambda code references api.bls.gov
- `22:53:06`              purpose: Employment, unemployment, JOLTS
- `22:53:06`   ❌ MISSING    Census                    → no Lambda code references api.census.gov
- `22:53:06`              purpose: Trade balance, retail sales
- `22:53:06`   ✅ ACTIVE      OFR                       → fmp-stock-picks-agent(2x)
- `22:53:06`              purpose: Financial Stress Index, money market
- `22:53:06`   ❌ MISSING    EIA                       → no Lambda code references api.eia.gov
- `22:53:06`              purpose: Oil/gas inventories, electricity
- `22:53:06`   ✅ ACTIVE      Polygon.io                → justhodl-khalid-metrics(1x), justhodl-stock-analyzer(1x), justhodl-bloomberg-v8(1x) + 8 more
- `22:53:06`              purpose: Stock prices, options contracts, ETF flows, news
- `22:53:06`   ✅ ACTIVE      AlphaVantage              → justhodl-stock-analyzer(1x), justhodl-options-flow(1x), alphavantage-technical-analysis(1x)
- `22:53:06`              purpose: Stock OHLC fallback (TIME_SERIES_WEEKLY_ADJUSTED)
- `22:53:06`   ✅ ACTIVE      FMP (Premium)             → justhodl-telegram-bot(1x), justhodl-stock-analyzer(1x), justhodl-news-sentiment(1x) + 3 more
- `22:53:06`              purpose: S&P 500 fundamentals, F-scores, Z-scores, earnings
- `22:53:06`   ✅ ACTIVE      CoinGecko                 → justhodl-daily-report-v3(2x), justhodl-ai-chat(1x), justhodl-crypto-intel(4x)
- `22:53:06`              purpose: Crypto prices, OHLC, market cap (USA-friendly)
- `22:53:06`   ✅ ACTIVE      CoinMarketCap             → justhodl-bloomberg-v8(1x), justhodl-financial-secretary(2x), justhodl-valuations-agent(1x) + 1 more
- `22:53:06`              purpose: Crypto trending, gainers/losers
- `22:53:06`   ✅ ACTIVE      Alternative.me            → justhodl-edge-engine(1x), justhodl-financial-secretary(1x), justhodl-crypto-intel(2x)
- `22:53:06`              purpose: Fear & Greed Index
- `22:53:06`   ✅ ACTIVE      DeFiLlama                 → justhodl-crypto-enricher(3x), justhodl-crypto-intel(2x)
- `22:53:06`              purpose: TVL, DEX volumes, yield rates
- `22:53:06`   ⚠ DEGRADED    Binance Futures           → justhodl-crypto-enricher(2x), justhodl-crypto-intel(2x)
- `22:53:06`              purpose: BTC/ETH/SOL open interest [BLOCKED in US-East-1]
- `22:53:06`   ⚠ DEGRADED    Binance Spot              → justhodl-crypto-enricher(2x), justhodl-crypto-intel(3x)
- `22:53:06`              purpose: klines for technicals [BLOCKED in US-East-1]
- `22:53:06`   ✅ ACTIVE      OKX                       → justhodl-crypto-intel(2x)
- `22:53:06`              purpose: Funding rates fallback
- `22:53:06`   ✅ ACTIVE      CFTC SODA                 → cftc-futures-positioning-agent(8x)
- `22:53:06`              purpose: COT 29 contracts (weekly Friday)
- `22:53:06`   ⚠ DEGRADED    Blockchain.info           → justhodl-crypto-intel(3x)
- `22:53:06`              purpose: BTC mempool whale txns [DESIGN ISSUE — mempool only]
- `22:53:06`   ✅ ACTIVE      Blocknative               → justhodl-crypto-enricher(1x)
- `22:53:06`              purpose: ETH gas fees
- `22:53:06`   ✅ ACTIVE      NewsAPI                   → justhodl-bloomberg-v8(1x), justhodl-news-sentiment(1x), justhodl-daily-report-v3(1x) + 1 more
- `22:53:06`              purpose: Financial headlines
- `22:53:06`   ❌ MISSING    CNN Fear/Greed            → no Lambda code references production.dataviz.cnn.io
- `22:53:06`              purpose: CNN sentiment index
- `22:53:06`   ❌ MISSING    DexScreener               → no Lambda code references api.dexscreener.com
- `22:53:06`              purpose: DEX trades, liquidity
- `22:53:06`   ✅ ACTIVE      Anthropic Claude API      → justhodl-telegram-bot(1x), justhodl-khalid-metrics(1x), justhodl-morning-intelligence(1x) + 6 more
- `22:53:06`              purpose: AI briefings, analysis, chat (claude-haiku-4-5-20251001)
## 2. Health by category

- `22:53:06`   ai              1/1 active
- `22:53:06`   crypto          4/4 active
- `22:53:06`   derivatives     1/3 active, 2 degraded
- `22:53:06`   dex             0/1 active
- `22:53:06`   futures         1/1 active
- `22:53:06`   macro           4/9 active
- `22:53:06`   market          3/3 active
- `22:53:06`   news            1/2 active
- `22:53:06`   onchain         1/2 active, 1 degraded
## 3. Khalid Index — what feeds the composite score?

## 4. DynamoDB — justhodl-signals table (signal types being tracked)

- `22:53:06`   Total scanned: 20 (limit 20)
- `22:53:06`   Unique signal types in last 20 entries:
- `22:53:06`     - carry_risk
- `22:53:06`     - edge_composite
- `22:53:06`     - edge_regime
- `22:53:06`     - market_phase
- `22:53:06`     - screener_top_pick
- `22:53:06` 
  Table total: 4579 items
## 5. Calibration loop — signals → outcomes → weights

- `22:53:06`   justhodl-outcomes: 10 recent items
- `22:53:06`     ? | crypto_risk_score | correct=?
- `22:53:06`     ? | crypto_risk_score | correct=?
- `22:53:06`     ? | crypto_fear_greed | correct=?
- `22:53:06` 
  /justhodl/calibration/weights (12 signals):
- `22:53:06`     cftc_bitcoin                   = 0.75
- `22:53:06`     cftc_crude                     = 0.7
- `22:53:06`     cftc_gold                      = 0.8
- `22:53:06`     cftc_spx                       = 0.8
- `22:53:06`     crypto_btc_signal              = 0.7
- `22:53:06`     crypto_eth_signal              = 0.65
- `22:53:06`     crypto_fear_greed              = 0.3098
- `22:53:06`     crypto_risk_score              = 0.3098
- `22:53:06`     edge_regime                    = 0.75
- `22:53:06`     khalid_index                   = 1.0
- `22:53:06`     screener_top_pick              = 0.85
- `22:53:06`     valuation_composite            = 0.8
- `22:53:06` 
  Last calibration update: 2026-04-19T09:00:47.859000+00:00 (5 days ago)
- `22:53:06` ✅   ✓ Calibration is active
- `22:53:06` 
  Per-signal accuracy (2 signals):
- `22:53:06`     crypto_fear_greed              accuracy=0.0, n=369
- `22:53:06`     crypto_risk_score              accuracy=0.0, n=369
## 6. Final summary

- `22:53:06`   Active sources:    16
- `22:53:06`   Degraded sources:  3 (Binance + blockchain.info — diagnosed)
- `22:53:06`   Missing sources:   7
- `22:53:06`   Total cataloged:   26
- `22:53:06` 
- `22:53:06`   Key conclusion: classify per source; tally calibration loop status.
- `22:53:06` Done
