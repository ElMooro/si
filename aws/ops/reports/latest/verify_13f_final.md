# Verify 13F positions after all fixes

**Status:** success  
**Duration:** 10.1s  
**Finished:** 2026-05-03T17:06:26+00:00  

## Log
## Invoke 13f-positions

- `17:06:24` ✅   invoke status: 200
- `17:06:24`   response body: {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": "{\"ok\": true, \"funds_parsed\": 16, \"funds_failed\": 1, \"tickers_aggregated\": 7525}"}
## Read data/13f-positions.json

- `17:06:26`   generated_at: 2026-05-03T17:06:24+00:00
- `17:06:26`   funds_parsed: 16 / 18
- `17:06:26`   funds_failed: 1
## Per-fund breakdown

- `17:06:26`   BERKSHIRE        42 positions  AUM $274160.1B
- `17:06:26`   DURATION       2311 positions  AUM $168006.6B
- `17:06:26`   TWO_SIGMA      3534 positions  AUM $ 70897.7B
- `17:06:26`   RENAISSANCE    3185 positions  AUM $ 64461.2B
- `17:06:26`   BRIDGEWATER    1040 positions  AUM $ 27421.6B
- `17:06:26`   CITADEL        6510 positions  AUM $   665.9B
- `17:06:26`   AQR            3562 positions  AUM $   190.6B
- `17:06:26`   POINT72        2549 positions  AUM $    89.4B
- `17:06:26`   COATUE           52 positions  AUM $    40.0B
- `17:06:26`   TIGER_GLOBAL     54 positions  AUM $    29.7B
- `17:06:26`   PERSHING         11 positions  AUM $    15.5B
- `17:06:26`   BAUPOST          32 positions  AUM $    13.6B
- `17:06:26`   SOROS           237 positions  AUM $     8.6B
- `17:06:26`   LONE_PINE        22 positions  AUM $     5.3B
- `17:06:26`   GREENLIGHT       40 positions  AUM $     2.0B
- `17:06:26`   SCION             8 positions  AUM $     1.4B
## Remaining errors

- `17:06:26`   MILLENNIUM: infotable_not_found
## Top 5 most-bought (cross-fund)

- `17:06:26`   AAPL     Apple Inc                                8 funds buying
- `17:06:26`   AXP      American Express                         7 funds buying
- `17:06:26`   KO       Coca-Cola Co                             14 funds buying
- `17:06:26`   BAC      Bank of America                          11 funds buying
- `17:06:26`   CVX      Chevron                                  6 funds buying
## Top 5 most-sold (cross-fund)

- `17:06:26`   AAPL     Apple Inc                                0 funds selling
- `17:06:26`   AXP      American Express                         0 funds selling
- `17:06:26`   BAC      Bank of America                          0 funds selling
- `17:06:26`   KO       Coca-Cola Co                             0 funds selling
- `17:06:26`   CVX      Chevron                                  0 funds selling
## Summary

- `17:06:26` ✅   ✅ 16/18 funds parsed — system operational
