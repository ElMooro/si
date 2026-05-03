# Wipe 13f-cache and force fresh parse

**Status:** success  
**Duration:** 14.1s  
**Finished:** 2026-05-03T17:13:16+00:00  

## Log
## 1. List existing cache files

- `17:13:03`   found 16 cached files
- `17:13:03`     versioned cache files: 0
- `17:13:03`     unversioned cache files: 16
## 2. Delete all 13f-cache files

- `17:13:03` ✅   ✓ deleted 16 cache files
## 3. Invoke 13f-positions Lambda for fresh parse

- `17:13:13`   invocation status: 200, duration: 10.3s
- `17:13:13`   response (first 400): {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": "{\"ok\": true, \"funds_parsed\": 16, \"funds_failed\": 1, \"tickers_aggregated\": 7524}"}
## 4. Read fresh data/13f-positions.json

- `17:13:16`   generated_at: 2026-05-03T17:13:13+00:00
- `17:13:16`   funds_parsed: 16 / 18
- `17:13:16`   funds_failed: 1
- `17:13:16`     MILLENNIUM      infotable_not_found
- `17:13:16` 
  Per-fund AUM (sanity check):
- `17:13:16`     BERKSHIRE           0 pos  AUM $      0.0B
- `17:13:16`     BRIDGEWATER         0 pos  AUM $      0.0B
- `17:13:16`     RENAISSANCE         0 pos  AUM $      0.0B
- `17:13:16`     AQR                 0 pos  AUM $      0.0B
- `17:13:16`     TWO_SIGMA           0 pos  AUM $      0.0B
- `17:13:16`     PERSHING            0 pos  AUM $      0.0B
- `17:13:16`     GREENLIGHT          0 pos  AUM $      0.0B
- `17:13:16`     CITADEL             0 pos  AUM $      0.0B
- `17:13:16`     TIGER_GLOBAL        0 pos  AUM $      0.0B
- `17:13:16`     COATUE              0 pos  AUM $      0.0B
- `17:13:16`     SOROS               0 pos  AUM $      0.0B
- `17:13:16`     BAUPOST             0 pos  AUM $      0.0B
- `17:13:16`     SCION               0 pos  AUM $      0.0B
- `17:13:16`     POINT72             0 pos  AUM $      0.0B
- `17:13:16`     LONE_PINE           0 pos  AUM $      0.0B
- `17:13:16`     DURATION            0 pos  AUM $      0.0B
- `17:13:16` 
  Top 5 most-bought (cross-fund):
- `17:13:16`     AAPL     Apple Inc                      14 funds, $93.5B total
- `17:13:16`     AMZN     Amazon.com Inc                 14 funds, $32.7B total
- `17:13:16`     GOOGL    Alphabet Inc Class A           12 funds, $30.4B total
- `17:13:16`     PEP      PepsiCo                        12 funds, $3.2B total
- `17:13:16`     NVDA     Nvidia                         11 funds, $53.4B total
