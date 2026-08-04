# ops 4352 -- shadows born, sources mapped

**Status:** success  
**Duration:** 22.6s  
**Finished:** 2026-08-04T02:54:30+00:00  

## Log
- `02:54:09` ✅ SOURCE-INTEL: integrated=9 · free_unused=5 · cheap=2
## free & unused (auto-adopt queue)

- `02:54:09`   Alpaca           free IEX-feed data w/ brokerage acct probe=-
- `02:54:09`   Alpha Vantage    free key; 25 req/d probe=-
- `02:54:09`   Binance          free public API probe=-
- `02:54:09`   CoinGecko        free tier 10k/mo probe=LIVE(200)
- `02:54:09`   Coinbase         free public API probe=-
## cheap & worth it (Khalid's buy list)

- `02:54:09`   Finnhub        $0/mo -- free tier 60/min; paid $50+
- `02:54:09`   Tiingo         $10/mo -- $10/mo EOD+news+fundamentals
- `02:54:09` teachers readable: 4/6 · indicators=63
- `02:54:10` donor FMP-ish vars: ['FMP_KEY'] -> key_len=32
- `02:54:25` env verified on readback (key_len=32)
- `02:54:30` engine debug_key_len=32
- `02:54:30` ✅ SHADOW-LAB: computed=40 · logged=11 · roster=['ADX', 'ATR', 'BOLLINGER', 'EMA', 'MACD', 'OBV']
- `02:54:30`   AFRI   ADX=62.4 +DI/-DI=25.8/8.2 ATRpct=3.6 pctile=93.0 sig=shadow-adx-trend
- `02:54:30`   WIZEY  ADX=43.4 +DI/-DI=29.8/9.9 ATRpct=4.67 pctile=100.0 sig=shadow-adx-trend
- `02:54:30`   ARCT   ADX=31.1 +DI/-DI=12.9/24.6 ATRpct=5.83 pctile=2.0 sig=shadow-adx-trend
- `02:54:30`   AAPL   ADX=29.0 +DI/-DI=24.7/34.2 ATRpct=3.23 pctile=100.0 sig=shadow-adx-trend
- `02:54:30`   ZURVY  ADX=26.4 +DI/-DI=35.3/23.4 ATRpct=1.17 pctile=13.0 sig=shadow-adx-trend
- `02:54:30` ✅ LEDGER: shadow-adx-trend#AFRI#2026-08-04 born, stamped=True (fabric_agreement=100)
- `02:54:30` ✅ OPS 4352 PASS -- borrowed ideas now compute, sign the ledger at birth, and queue for their Wilson trial; the source atlas prices Khalid's next unlocks
