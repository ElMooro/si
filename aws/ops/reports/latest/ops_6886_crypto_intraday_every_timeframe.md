# ops 6886 - crypto intraday through the data proxy, every timeframe (read-only), 2026-11-21T19:04+00:00

**Status:** success  
**Duration:** 105.4s  
**Finished:** 2026-11-21T19:05:45+00:00  

## Data

| failed | ok |
|---|---|
| 12 | 28 |

## Log
## 1. The proxy's /ohlc

- `19:04:08` BTC   1 minute (   7 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:04:14` BTC   3 minute (  30 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:04:15` BTC   5 minute (  30 d) OK   | try 1: HTTP 200  1500 bars, spacing 5 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:16` BTC  15 minute (  90 d) OK   | try 1: HTTP 200  1800 bars, spacing 15 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:16` BTC  30 minute ( 180 d) OK   | try 1: HTTP 200   901 bars, spacing 30 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:17` BTC  45 minute ( 180 d) OK   | try 1: HTTP 200   601 bars, spacing 45 min, newest 19 min old, coinbase public market data cache MISS
- `19:04:18` BTC   1 hour   ( 730 d) OK   | try 1: HTTP 200  1800 bars, spacing 60 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:19` BTC   2 hour   ( 730 d) OK   | try 1: HTTP 200   900 bars, spacing 120 min, newest 64 min old, coinbase public market data cache MISS
- `19:04:20` BTC   4 hour   (1500 d) OK   | try 1: HTTP 200   450 bars, spacing 240 min, newest 184 min old, coinbase public market data cache MISS
- `19:04:20` BTC  12 hour   (2000 d) OK   | try 1: HTTP 200   600 bars, spacing 720 min, newest 424 min old, coinbase public market data cache MISS
- `19:04:28` ETH   1 minute (   7 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:04:34` ETH   3 minute (  30 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:04:40` ETH   5 minute (  30 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:04:40` ETH  15 minute (  90 d) OK   | try 1: HTTP 200  1800 bars, spacing 15 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:41` ETH  30 minute ( 180 d) OK   | try 1: HTTP 200   901 bars, spacing 30 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:42` ETH  45 minute ( 180 d) OK   | try 1: HTTP 200   601 bars, spacing 45 min, newest 19 min old, coinbase public market data cache MISS
- `19:04:43` ETH   1 hour   ( 730 d) OK   | try 1: HTTP 200  1200 bars, spacing 60 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:43` ETH   2 hour   ( 730 d) OK   | try 1: HTTP 200   750 bars, spacing 120 min, newest 64 min old, coinbase public market data cache MISS
- `19:04:44` ETH   4 hour   (1500 d) OK   | try 1: HTTP 200   450 bars, spacing 240 min, newest 184 min old, coinbase public market data cache MISS
- `19:04:45` ETH  12 hour   (2000 d) OK   | try 1: HTTP 200   900 bars, spacing 720 min, newest 424 min old, coinbase public market data cache MISS
- `19:04:47` SOL   1 minute (   7 d) OK   | try 1: HTTP 200   300 bars, spacing 1 min, newest 0 min old, coinbase public market data cache MISS
- `19:04:47` SOL   3 minute (  30 d) OK   | try 1: HTTP 200   101 bars, spacing 3 min, newest 1 min old, coinbase public market data cache MISS
- `19:04:53` SOL   5 minute (  30 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:04:54` SOL  15 minute (  90 d) OK   | try 1: HTTP 200   600 bars, spacing 15 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:54` SOL  30 minute ( 180 d) OK   | try 1: HTTP 200   451 bars, spacing 30 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:55` SOL  45 minute ( 180 d) OK   | try 1: HTTP 200   301 bars, spacing 45 min, newest 19 min old, coinbase public market data cache MISS
- `19:04:56` SOL   1 hour   ( 730 d) OK   | try 1: HTTP 200  1500 bars, spacing 60 min, newest 4 min old, coinbase public market data cache MISS
- `19:04:57` SOL   2 hour   ( 730 d) OK   | try 1: HTTP 200   750 bars, spacing 120 min, newest 64 min old, coinbase public market data cache MISS
- `19:04:57` SOL   4 hour   (1500 d) OK   | try 1: HTTP 200   450 bars, spacing 240 min, newest 184 min old, coinbase public market data cache MISS
- `19:04:58` SOL  12 hour   (2000 d) OK   | try 1: HTTP 200   600 bars, spacing 720 min, newest 424 min old, coinbase public market data cache MISS
- `19:05:05` TON   1 minute (   7 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:05:08` TON   3 minute (  30 d) OK   | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200    90 bars, spacing 3 min, newest 1 min old, coinbase public market data cache MISS
- `19:05:14` TON   5 minute (  30 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:05:20` TON  15 minute (  90 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:05:23` TON  30 minute ( 180 d) OK   | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200   151 bars, spacing 30 min, newest 4 min old, coinbase public market data cache MISS
- `19:05:24` TON  45 minute ( 180 d) OK   | try 1: HTTP 200   101 bars, spacing 45 min, newest 19 min old, coinbase public market data cache MISS
- `19:05:30` TON   1 hour   ( 730 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:05:36` TON   2 hour   ( 730 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:05:37` TON   4 hour   (1500 d) OK   | try 1: HTTP 200   300 bars, spacing 240 min, newest 184 min old, coinbase public market data cache MISS
- `19:05:42` TON  12 hour   (2000 d) FAIL | try 1: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache MISS || try 2: HTTP 200     0 bars, spacing - min, newest - min old, polygon (warehouse empty) cache HIT
- `19:05:42` timeframes answered with bars of the asked spacing and a current newest bar: 28 of 40
## 2. Coinbase itself, the same windows (from this runner)

- `19:05:42`   granularity    60, start/end 300 candles wide: HTTP 200, 299 rows
- `19:05:43`   granularity    60, no window: HTTP 200, 350 rows
- `19:05:43`   granularity   300, start/end 300 candles wide: HTTP 200, 301 rows
- `19:05:43`   granularity   300, no window: HTTP 200, 350 rows
- `19:05:44`   granularity   900, start/end 300 candles wide: HTTP 200, 300 rows
- `19:05:44`   granularity   900, no window: HTTP 200, 350 rows
- `19:05:44`   granularity  3600, start/end 300 candles wide: HTTP 200, 300 rows
- `19:05:44`   granularity  3600, no window: HTTP 200, 350 rows
- `19:05:45`   granularity 21600, start/end 300 candles wide: HTTP 200, 300 rows
- `19:05:45`   granularity 21600, no window: HTTP 200, 350 rows
- `19:05:45` ✅ read-only; nothing changed
