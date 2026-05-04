# Probe FINRA fetch from inside Lambda

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-05-04T00:11:15+00:00  

## Log
## 1. Read recent Lambda logs

- `00:11:14`   stream: 2026/05/04/[$LATEST]04b26d3100b24922a1c4bda084e9d77a
- `00:11:14`     INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e4ab553846c4e081013ff7d1d608a5358d5b956bb5b81c83c66d2a31da8f6244
- `00:11:14`     [short-interest] start — watchlist=158 tickers
- `00:11:14`     [short-interest] FINRA: 0 tickers w/ short volume data
- `00:11:14`     [short-interest] Polygon: 157 tickers w/ short interest snapshot
- `00:11:14`     [short-interest] wrote s3://justhodl-dashboard-live/data/short-interest.json — 48,322b in 3.98s
- `00:11:14`     [short-interest] start — watchlist=158 tickers
- `00:11:14`     [short-interest] FINRA: 0 tickers w/ short volume data
- `00:11:14`     [short-interest] Polygon: 157 tickers w/ short interest snapshot
- `00:11:14`     [short-interest] wrote s3://justhodl-dashboard-live/data/short-interest.json — 48,322b in 3.96s
- `00:11:14`     [short-interest] start — watchlist=158 tickers
- `00:11:14`     [short-interest] FINRA: 0 tickers w/ short volume data
- `00:11:14`     [short-interest] Polygon: 157 tickers w/ short interest snapshot
- `00:11:14`     [short-interest] wrote s3://justhodl-dashboard-live/data/short-interest.json — 48,322b in 4.51s
## 2. Inline test of urlopen via a one-shot Lambda invoke

- `00:11:15`   ✓ from runner: status=200 size=505,143 duration=0.20s
