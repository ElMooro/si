# Probe AI Lambda Function URL via public HTTPS

**Status:** success  
**Duration:** 4.8s  
**Finished:** 2026-04-25T23:52:24+00:00  

## Log
## A. Create temporary probe Lambda

- `23:52:19` ✅   Created justhodl-tmp-url-probe
## B. Probe with browser-like Origin: justhodl.ai

- `23:52:23`   Probe result: ok=True
- `23:52:23`   Status: 200
- `23:52:23`   Body length: 4089
- `23:52:23` 
- `23:52:23`   Response headers:
- `23:52:23`     Date: Sat, 25 Apr 2026 23:52:23 GMT
- `23:52:23`     Content-Type: application/json
- `23:52:23`     Content-Length: 4089
- `23:52:23`     Connection: close
- `23:52:23`     x-amzn-RequestId: 7ac9b653-92f0-42ff-92f7-65e407963f58
- `23:52:23`     access-control-allow-origin: *
- `23:52:23`     access-control-allow-headers: Content-Type,Authorization
- `23:52:23`     Vary: Origin
- `23:52:23`     access-control-allow-methods: GET,POST,OPTIONS
- `23:52:23`     X-Amzn-Trace-Id: Root=1-69ed53b7-4c02bfd32db4b6d01e6ecfbc;Parent=3369b828dc48dcdf;Sampled=0;Lineage=1:e0649372:0
- `23:52:23` 
- `23:52:23`   Body preview:
- `23:52:23`     {"ticker": "AAPL", "company": {"name": "Apple Inc.", "sector": "Technology", "industry": "Consumer Electronics", "ceo": "Timothy D. Cook", "employees": 164000.0, "ipo_date": "1980-12-12", "website": "https://www.apple.com", "country": "US", "exchange": "NASDAQ"}, "snapshot": {"price": 271.06, "market_cap": 3979469913842.0, "pe": 33.9, "ps": 9.14, "pb": 45.33, "ev_ebitda": 26.3, "roe": 1.599, "roic": 0.51, "net_margin": 0.27, "rev_growth": 0.064, "chg_1m": 7.3, "chg_3m": 9.28, "chg_1y": 29.52, "analyst_target_median": 325.0}, "ai": {"description": "Apple designs, manufactures, and sells iPhones, Macs, iPads, wearables, and services globally. Revenue is driven by hardware sales (iPhone ~52% of revenue) and recurring services (App Store, AppleCare, cloud) which command 27% net margins.", "bul
## C. Probe OPTIONS preflight (what browser sends first)

- `23:52:23`   OPTIONS preflight: ok=True status=200
- `23:52:23`     Date: Sat, 25 Apr 2026 23:52:23 GMT
- `23:52:23`     Content-Type: application/json
- `23:52:23`     Content-Length: 0
- `23:52:23`     Connection: close
- `23:52:23`     x-amzn-RequestId: 5edf82a2-3457-45a4-a2fe-1a4a19d89ea4
- `23:52:23`     Access-Control-Allow-Origin: https://justhodl.ai
- `23:52:23`     Access-Control-Allow-Headers: *
- `23:52:23`     Vary: Origin
- `23:52:23`     Access-Control-Allow-Methods: *
- `23:52:23`     Access-Control-Max-Age: 86400
## D. Cleanup probe Lambda

- `23:52:24` ✅   Deleted justhodl-tmp-url-probe
- `23:52:24` Done
