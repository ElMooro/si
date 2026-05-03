# Full 13F verification — all funds + change detection

**Status:** success  
**Duration:** 27.7s  
**Finished:** 2026-05-03T17:22:25+00:00  

## Log
## 1. Invoke sec-13f to refresh filings index (with prior_filing field)

- `17:21:59` ✅   status: 200, dur: 1.8s
## 2. Wipe 13f-cache to force re-parse with new logic

- `17:22:01`   found 16 cached files
- `17:22:02` ✅   ✓ deleted 16
## 3. Invoke 13f-positions (allow up to 10 min for full parse)

- `17:22:21`   status: 200, dur: 19.4s
- `17:22:21`   body: {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": "{\"ok\": true, \"funds_parsed\": 17, \"funds_failed\": 0, \"tickers_aggregated\": 7940}"}
## 4. Final state

- `17:22:25`   funds_parsed: 17 / 18
- `17:22:25`   funds_failed: 0
## 5. AUMs (sanity)

- `17:22:25`     CITADEL          6510p $  671.7B   NEW= 15 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     BERKSHIRE          42p $  274.2B   NEW=  4 EXIT=  3 ADD=  9 TRIM= 15
- `17:22:25`     MILLENNIUM       4328p $  250.1B   NEW= 15 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     AQR              3562p $  190.7B   NEW= 15 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     DURATION         2311p $  168.0B   NEW= 15 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     POINT72          2549p $   90.7B   NEW= 15 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     TWO_SIGMA        3534p $   77.2B   NEW= 15 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     RENAISSANCE      3185p $   72.7B   NEW= 15 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     COATUE             52p $   40.0B   NEW= 13 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     TIGER_GLOBAL       54p $   29.7B   NEW=  1 EXIT=  3 ADD= 15 TRIM= 15
- `17:22:25`     BRIDGEWATER      1040p $   27.4B   NEW= 15 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     PERSHING           11p $   15.5B   NEW=  1 EXIT=  1 ADD=  4 TRIM=  4
- `17:22:25`     BAUPOST            32p $   13.6B   NEW= 14 EXIT=  7 ADD=  6 TRIM=  5
- `17:22:25`     SOROS             237p $    8.6B   NEW= 15 EXIT= 15 ADD= 15 TRIM= 15
- `17:22:25`     LONE_PINE          22p $    5.3B   NEW=  3 EXIT=  3 ADD=  7 TRIM=  7
- `17:22:25`     GREENLIGHT         40p $    2.1B   NEW=  7 EXIT=  6 ADD= 15 TRIM=  8
- `17:22:25`     SCION               8p $    1.4B   NEW=  7 EXIT= 10 ADD=  0 TRIM=  1
## 6. Top changes (cross-fund)

- `17:22:25` 
  Most bought (top 8 by buying activity):
- `17:22:25`     AAPL     Apple Inc                      +10 buying / -2 selling
- `17:22:25`     AMZN     Amazon.com Inc                 +10 buying / -3 selling
- `17:22:25`     GOOGL    Alphabet Inc Class A           +9 buying / -4 selling
- `17:22:25`     AVGO     Broadcom Inc                   +9 buying / -0 selling
- `17:22:25`     007903107 ADVANCED MICRO DEVICES INC     +9 buying / -1 selling
- `17:22:25`     PEP      PepsiCo                        +9 buying / -3 selling
- `17:22:25`     874039100 TAIWAN SEMICONDUCTOR MFG LTD   +8 buying / -2 selling
- `17:22:25`     595112103 MICRON TECHNOLOGY INC          +8 buying / -0 selling
- `17:22:25` 
  Most sold (top 8 by selling activity):
- `17:22:25`     META     Meta Platforms                 +2 buying / -9 selling
- `17:22:25`     03831W108 APPLOVIN CORP                  +2 buying / -9 selling
- `17:22:25`     MSFT     Microsoft                      +1 buying / -8 selling
- `17:22:25`     81141R100 SEA LTD                        +2 buying / -8 selling
- `17:22:25`     98980G102 ZSCALER INC                    +1 buying / -8 selling
- `17:22:25`     WYNN     Wynn Resorts                   +0 buying / -7 selling
- `17:22:25`     75734B100 REDDIT INC                     +3 buying / -7 selling
- `17:22:25`     90353T100 UBER TECHNOLOGIES INC          +4 buying / -7 selling
- `17:22:25` ✅ 
  ✅ 17/18 funds operational
