- `18:14:17` G0 producer contract OK
- `18:14:18` async invoke rc=202 (202 expected)
**Status:** failure  
**Duration:** 392.7s  
**Finished:** 2026-08-20T18:20:50+00:00  

## Error

```
SystemExit: 1
```

## Log
- `18:20:43` payload refreshed after 386s (age 20s)
- `18:20:50` PASS G1 one ticker <- exactly one cusip  []
- `18:20:50` PASS G2 major names hold their own tickers again  []
- `18:20:50` unresolved top: [{"k": "428040DE6", "name": "HERTZ CORP", "usd": 8.63}, {"k": "084670702", "name": "BERKSHIRE HATHAWAY INC DEL", "usd": 5.2}, {"k": "958102105", "name": "WESTERN DIGITAL CORP", "usd": 3.68}, {"k": "90353T100", "name": "UBER TECHNOLOGIES INC", "usd": 2.99}, {"k": "833445109", "name": "SNOWFLAKE INC", "usd": 2.97}, {"k": "81141R100", "name": "SEA LTD", "usd": 2.36}, {"k": "55306N104", "name": "MKS INC.", "usd": 1.92}, {"k": "526057104", "name": "Lennar Corporation", "usd": 1.8}]
- `18:20:50` FAIL G3 unresolved book back under $15B (was $70.68B)  $88.04B
- `18:20:50` PASS G4a roster balances  18/17/1
- `18:20:50` PASS G4b holders <= roster  
- `18:20:50` PASS G4c exits <= roster  
- `18:20:50` PASS G4d mcap_suspect still published honestly  4
- `18:20:50` as_of=2026-06-30 parsed=17/18 tickers=8666
- `18:20:50` ops 4938 RED: G3 unresolved book back under $15B (was $70.68B)
