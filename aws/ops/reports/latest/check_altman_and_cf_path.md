# Verify Altman + Cloudflare Worker path

**Status:** success  
**Duration:** 6.8s  
**Finished:** 2026-04-26T00:12:06+00:00  

## Data

| altman_n | altman_pct | n_stocks |
|---|---|---|
| 494 | 98.2 | 503 |

## Log
## A. Screener cache: current altmanZ state

- `00:12:00`   Cache mtime: 2026-04-26 00:09:02+00:00
- `00:12:00`   Total stocks: 503
- `00:12:00`   altmanZ: 494/503 (98.2%)
- `00:12:00`   sma50:   499/503 (99.2%)
- `00:12:00` ✅   ✅ altmanZ populated for 494 stocks
- `00:12:00` 
  Top 5 safest:
- `00:12:00`     UNP    Industrials          Z=2188.95
- `00:12:00`     PLTR   Technology           Z=140.73
- `00:12:00`     TPL    Energy               Z=112.04
- `00:12:00`     MPWR   Technology           Z= 75.22
- `00:12:00`     NVDA   Technology           Z= 66.19
- `00:12:00` 
  Bottom 5:
- `00:12:00`     KEY    Financial Services   Z= -0.27  Distress
- `00:12:00`     WFC    Financial Services   Z= -0.32  Distress
- `00:12:00`     PNC    Financial Services   Z= -0.32  Distress
- `00:12:00`     SATS   Technology           Z= -0.73  Distress
- `00:12:00`     VRSN   Technology           Z=-13.01  Distress
## B. Test https://api.justhodl.ai/research?ticker=AAPL

- `00:12:04` 
  GET test:
- `00:12:06` ✅   ✅ Status 200, body 4089 bytes
- `00:12:06`     Access-Control-Allow-Origin: https://justhodl.ai
- `00:12:06`     Content-Type: application/json
- `00:12:06`     Vary: Origin
- `00:12:06` 
    Company: Apple Inc.
- `00:12:06`     Description: Apple designs, manufactures, and sells iPhones, Macs, iPads, wearables, and services globally. Revenue is driven by hardware sales (iPhone ~52% of rev
- `00:12:06`     Bull thesis: Services acceleration and installed-base monetization justify premium valuation despite hardware maturity.
- `00:12:06` 
  OPTIONS preflight test:
- `00:12:06`   (skipping — if GET worked with proper CORS header, preflight already works)
- `00:12:06` Done
