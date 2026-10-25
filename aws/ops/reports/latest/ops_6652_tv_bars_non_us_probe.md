# ops 6652 - chart names stuck + what the FMP banks hold for them (read-only)

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-10-25T19:15:43+00:00  

## Log
- `19:15:43` index: n_symbols 309 updated_at 2026-10-25T02:30:04+00:00 polygon_append.through 2026-10-23 keys ['failures', 'n_symbols', 'polygon_append', 'symbols', 'updated_at']
## names whose newest banked bar is older than 2026-10-20 (21)

- `19:15:43` X:PEPEUSD ns=X last=2022-12-11 bars=119  <- non-US namespace
- `19:15:43` US:NGE ns=US last=2024-03-28 bars=2768
- `19:15:43` US:FM ns=US last=2025-01-08 bars=3100
- `19:15:43` SSE:000001 ns=SSE last=2026-09-30 bars=7438  <- non-US namespace
- `19:15:43` FX:EURUSD ns=FX last=2026-10-02 bars=6045  <- non-US namespace
- `19:15:43` CME_MINI:ES1! ns=CME_MINI last=2026-10-02 bars=6664  <- non-US namespace
- `19:15:43` HKEX:700 ns=HKEX last=2026-10-02 bars=5774  <- non-US namespace
- `19:15:43` I:SPX ns=I last=2026-10-02 bars=24975  <- non-US namespace
- `19:15:43` XETR:DAX ns=XETR last=2026-10-02 bars=9959  <- non-US namespace
- `19:15:43` TVC:US10Y ns=TVC last=2026-10-02 bars=16176  <- non-US namespace
- `19:15:43` US:^MOVE ns=US last=2026-10-02 bars=5908
- `19:15:43` US:^TNX ns=US last=2026-10-02 bars=16176
- `19:15:43` US:^IRX ns=US last=2026-10-02 bars=16673
- `19:15:43` US:^TYX ns=US last=2026-10-02 bars=12434
- `19:15:43` I:NDX ns=I last=2026-10-02 bars=10331  <- non-US namespace
- `19:15:43` C:BRLUSD ns=C last=2026-10-03 bars=5509  <- non-US namespace
- `19:15:43` X:BTCUSD ns=X last=2026-10-04 bars=4402  <- non-US namespace
- `19:15:43` COINBASE:BTCUSD ns=COINBASE last=2026-10-04 bars=4402  <- non-US namespace
- `19:15:43` COINBASE:ETHUSD ns=COINBASE last=2026-10-04 bars=3252  <- non-US namespace
- `19:15:43` US:PSKY ns=US last=2026-10-05 bars=5240
- `19:15:43` US:HEPA ns=US last=2026-10-12 bars=3186
- `19:15:43` failures recorded: 4308 {"TVC:VIX": {"err": "all endpoints refused: data.tradingview.com/socket.io/websocket?from= -> handshake b'HTTP/1.1 400 Ba", "at": "2026-09-02T14:29:58+00:00"}, "NASDAQ:AAPL": {"err": "'int' object has no attribute 'get'", "at": "2026-09-02T14:42:25+00:00"}, "ECONOMICS:DEUR": {"err": "no bars for ECONOMICS:DEUR (tv:all endpoints refused: data.tradingview.com/socket.io/websocket?from=", "at": "2026-09-02T15:15:31+00:00"}, "SSE:000001": {"err": "'int' object has no attribute 'get'", "at": "2026-09-02T14:43:18+00:00"}, "FX:EURUSD": {"err": "'int' object has no attribute 'get'", "at": "2026-09-02T14:42:28+00:00"}, "COINBASE:BTCUSD": {"err": "'int' object has no attribute 'get'", "at": "2026-09-02T14:42:31+00:00"}, "AMEX:SPY": {"err": "'int' object has no attribute 'get'", "at": "2026-09-02T14:4
## FMP banks for the stuck non-US names

- `19:15:43` fmp-bank eod-bulk newest session banked: None (0 sessions listed)
- `19:15:43` X:PEPEUSD (last 2022-12-11): PEPEUSD: walker miss (newest None), eod-bulk miss
- `19:15:43` SSE:000001 (last 2026-09-30): 000001.SS: walker miss (newest None), eod-bulk miss
- `19:15:43` FX:EURUSD (last 2026-10-02): EURUSD: walker miss (newest None), eod-bulk miss
- `19:15:43` CME_MINI:ES1! (last 2026-10-02): ES1!: walker miss (newest None), eod-bulk miss
- `19:15:43` HKEX:700 (last 2026-10-02): 700.HK: walker miss (newest None), eod-bulk miss
- `19:15:43` I:SPX (last 2026-10-02): SPX: walker miss (newest None), eod-bulk miss; ^GSPC: walker miss (newest None), eod-bulk miss
- `19:15:43` XETR:DAX (last 2026-10-02): DAX.DE: walker miss (newest None), eod-bulk miss
- `19:15:43` TVC:US10Y (last 2026-10-02): US10Y: walker miss (newest None), eod-bulk miss; ^TNX: walker miss (newest None), eod-bulk miss
- `19:15:43` I:NDX (last 2026-10-02): NDX: walker miss (newest None), eod-bulk miss; ^NDX: walker miss (newest None), eod-bulk miss
- `19:15:43` C:BRLUSD (last 2026-10-03): BRLUSD: walker miss (newest None), eod-bulk miss
- `19:15:43` X:BTCUSD (last 2026-10-04): BTCUSD: walker miss (newest None), eod-bulk miss
- `19:15:43` COINBASE:BTCUSD (last 2026-10-04): BTCUSD: walker miss (newest None), eod-bulk miss
- `19:15:43` COINBASE:ETHUSD (last 2026-10-04): ETHUSD: walker miss (newest None), eod-bulk miss
- `19:15:43` ✅ ops 6652 complete (read-only)
