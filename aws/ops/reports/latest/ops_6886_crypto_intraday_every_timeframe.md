# ops 6886 - crypto intraday through the data proxy, every timeframe (read-only), 2026-11-21T19:20+00:00

**Status:** success  
**Duration:** 91.8s  
**Finished:** 2026-11-21T19:21:44+00:00  

## Data

| failed | ok |
|---|---|
| 3 | 37 |

## Log
## 1. The proxy's /ohlc

- `19:20:13` BTC   1 minute (   7 d) OK   | try 1: HTTP 200   899 bars, spacing 1 min, newest 1 min old, coinbase public market data cache MISS
- `19:20:14` BTC   3 minute (  30 d) OK   | try 1: HTTP 200   300 bars, spacing 3 min, newest 2 min old, coinbase public market data cache MISS
- `19:20:23` BTC   5 minute (  30 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, crypto intraday unavailable cache None || try 2: HTTP 200     0 bars, spacing - min, newest - min old, crypto intraday unavailable cache None
- `19:20:23` BTC  15 minute (  90 d) OK   | try 1: HTTP 200   900 bars, spacing 15 min, newest 5 min old, coinbase public market data cache MISS
- `19:20:24` BTC  30 minute ( 180 d) OK   | try 1: HTTP 200   450 bars, spacing 30 min, newest 20 min old, coinbase public market data cache MISS
- `19:20:24` BTC  45 minute ( 180 d) OK   | try 1: HTTP 200   300 bars, spacing 45 min, newest 35 min old, coinbase public market data cache MISS
- `19:20:25` BTC   1 hour   ( 730 d) OK   | try 1: HTTP 200  1200 bars, spacing 60 min, newest 20 min old, coinbase public market data cache MISS
- `19:20:26` BTC   2 hour   ( 730 d) OK   | try 1: HTTP 200   600 bars, spacing 120 min, newest 80 min old, coinbase public market data cache MISS
- `19:20:26` BTC   4 hour   (1500 d) OK   | try 1: HTTP 200   300 bars, spacing 240 min, newest 200 min old, coinbase public market data cache MISS
- `19:20:29` BTC  12 hour   (2000 d) OK   | try 1: HTTP 200   450 bars, spacing 720 min, newest 440 min old, coinbase public market data cache MISS
- `19:20:32` ETH   1 minute (   7 d) OK   | try 1: HTTP 200   600 bars, spacing 1 min, newest 0 min old, coinbase public market data cache MISS
- `19:20:33` ETH   3 minute (  30 d) OK   | try 1: HTTP 200   300 bars, spacing 3 min, newest 2 min old, coinbase public market data cache MISS
- `19:20:34` ETH   5 minute (  30 d) OK   | try 1: HTTP 200   900 bars, spacing 5 min, newest 0 min old, coinbase public market data cache MISS
- `19:20:35` ETH  15 minute (  90 d) OK   | try 1: HTTP 200   600 bars, spacing 15 min, newest 5 min old, coinbase public market data cache MISS
- `19:20:37` ETH  30 minute ( 180 d) OK   | try 1: HTTP 200   450 bars, spacing 30 min, newest 20 min old, coinbase public market data cache MISS
- `19:20:37` ETH  45 minute ( 180 d) OK   | try 1: HTTP 200   300 bars, spacing 45 min, newest 35 min old, coinbase public market data cache MISS
- `19:20:46` ETH   1 hour   ( 730 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, crypto intraday unavailable cache None || try 2: HTTP 200     0 bars, spacing - min, newest - min old, crypto intraday unavailable cache None
- `19:20:47` ETH   2 hour   ( 730 d) OK   | try 1: HTTP 200   150 bars, spacing 120 min, newest 80 min old, coinbase public market data cache MISS
- `19:20:49` ETH   4 hour   (1500 d) OK   | try 1: HTTP 200   300 bars, spacing 240 min, newest 200 min old, coinbase public market data cache MISS
- `19:20:56` ETH  12 hour   (2000 d) OK   | try 1: HTTP 200     0 bars, spacing - min, newest - min old, crypto intraday unavailable cache None || try 2: HTTP 200   300 bars, spacing 720 min, newest 440 min old, coinbase public market data cache MISS
- `19:20:57` SOL   1 minute (   7 d) OK   | try 1: HTTP 200   900 bars, spacing 1 min, newest 0 min old, coinbase public market data cache MISS
- `19:20:58` SOL   3 minute (  30 d) OK   | try 1: HTTP 200   300 bars, spacing 3 min, newest 2 min old, coinbase public market data cache MISS
- `19:21:00` SOL   5 minute (  30 d) OK   | try 1: HTTP 200   600 bars, spacing 5 min, newest 0 min old, coinbase public market data cache MISS
- `19:21:02` SOL  15 minute (  90 d) OK   | try 1: HTTP 200   900 bars, spacing 15 min, newest 5 min old, coinbase public market data cache MISS
- `19:21:02` SOL  30 minute ( 180 d) OK   | try 1: HTTP 200   450 bars, spacing 30 min, newest 20 min old, coinbase public market data cache MISS
- `19:21:02` SOL  45 minute ( 180 d) OK   | try 1: HTTP 200   300 bars, spacing 45 min, newest 35 min old, coinbase public market data cache MISS
- `19:21:04` SOL   1 hour   ( 730 d) OK   | try 1: HTTP 200  1200 bars, spacing 60 min, newest 20 min old, coinbase public market data cache MISS
- `19:21:05` SOL   2 hour   ( 730 d) OK   | try 1: HTTP 200   600 bars, spacing 120 min, newest 80 min old, coinbase public market data cache MISS
- `19:21:05` SOL   4 hour   (1500 d) OK   | try 1: HTTP 200   300 bars, spacing 240 min, newest 200 min old, coinbase public market data cache MISS
- `19:21:12` SOL  12 hour   (2000 d) OK   | try 1: HTTP 200     0 bars, spacing - min, newest - min old, crypto intraday unavailable cache None || try 2: HTTP 200   450 bars, spacing 720 min, newest 440 min old, coinbase public market data cache MISS
- `19:21:22` TON   1 minute (   7 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, crypto intraday unavailable cache None || try 2: HTTP 200   148 bars, spacing 2 min, newest 0 min old, coinbase public market data cache MISS
- `19:21:24` TON   3 minute (  30 d) OK   | try 1: HTTP 200   283 bars, spacing 3 min, newest 2 min old, coinbase public market data cache MISS
- `19:21:30` TON   5 minute (  30 d) OK   | try 1: HTTP 200     0 bars, spacing - min, newest - min old, crypto intraday unavailable cache None || try 2: HTTP 200   573 bars, spacing 5 min, newest 0 min old, coinbase public market data cache MISS
- `19:21:37` TON  15 minute (  90 d) OK   | try 1: HTTP 200     0 bars, spacing - min, newest - min old, crypto intraday unavailable cache None || try 2: HTTP 200   600 bars, spacing 15 min, newest 5 min old, coinbase public market data cache MISS
- `19:21:37` TON  30 minute ( 180 d) OK   | try 1: HTTP 200   450 bars, spacing 30 min, newest 20 min old, coinbase public market data cache MISS
- `19:21:38` TON  45 minute ( 180 d) OK   | try 1: HTTP 200   300 bars, spacing 45 min, newest 35 min old, coinbase public market data cache MISS
- `19:21:38` TON   1 hour   ( 730 d) OK   | try 1: HTTP 200  1200 bars, spacing 60 min, newest 20 min old, coinbase public market data cache MISS
- `19:21:39` TON   2 hour   ( 730 d) OK   | try 1: HTTP 200   600 bars, spacing 120 min, newest 80 min old, coinbase public market data cache MISS
- `19:21:39` TON   4 hour   (1500 d) OK   | try 1: HTTP 200   300 bars, spacing 240 min, newest 200 min old, coinbase public market data cache MISS
- `19:21:41` TON  12 hour   (2000 d) OK   | try 1: HTTP 200   600 bars, spacing 720 min, newest 440 min old, coinbase public market data cache MISS
- `19:21:41` timeframes answered with bars of the asked spacing and a current newest bar: 37 of 40
## 2. Coinbase itself, the same windows (from this runner)

- `19:21:41`   granularity    60, start/end 300 candles wide: HTTP 200, 299 rows
- `19:21:41`   granularity    60, no window: HTTP 200, 350 rows
- `19:21:42`   granularity   300, start/end 300 candles wide: HTTP 200, 300 rows
- `19:21:42`   granularity   300, no window: HTTP 200, 350 rows
- `19:21:42`   granularity   900, start/end 300 candles wide: HTTP 200, 300 rows
- `19:21:42`   granularity   900, no window: HTTP 200, 350 rows
- `19:21:43`   granularity  3600, start/end 300 candles wide: HTTP 200, 300 rows
- `19:21:43`   granularity  3600, no window: HTTP 200, 350 rows
- `19:21:43`   granularity 21600, start/end 300 candles wide: HTTP 200, 300 rows
- `19:21:43`   granularity 21600, no window: HTTP 200, 350 rows
- `19:21:44` ✅ read-only; nothing changed
