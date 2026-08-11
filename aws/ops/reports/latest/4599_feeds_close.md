# ops 4599 — surfaced feeds closed + fred blended rate

**Status:** failure  
**Duration:** 14.2s  
**Finished:** 2026-08-11T00:48:12+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Settle + sync-invoke the three

## 2. fpr — universe restored

- `00:48:10` ✗   [fpr] CONTRACT MISS — state=INSUFFICIENT_DATA universe=None scanned=0
## 3. 13f — feeder tickers flowing

- `00:48:12` ✅   [13f] consistency: feed dict=7023 extractor=7023 state=QUIET
## 4. skew — premium rows vs feed truth

- `00:48:12` ✅   [skew] consistency: feed qualifying rows=0 extractor names=0 state=INSUFFICIENT_DATA (feed keys: ['data', 'engine', 'legacy_alias_of', 'meta', 'success', 'timestamp'])
## 5. fred — v2.3 + blended rate since 00:38

- `00:48:12`   ver=2.2.1 rpm=60.0 imported=61551 (+561 in 9 min = 60.1/min = 3604/h) cursor=54561 throttled=270
- `00:48:12` ✗   [fred] CONTRACT MISS — v2.3 live on the chain
- `00:48:12` ✅   [fred] blended rate 60.1/min (serial era was 49; ceiling-only was 65.7)
- `00:48:12`   remaining≈220544 → ETA 61.2 h (~2.5 days)
## verdict

- `00:48:12` ✗ feeds close: 2 red
