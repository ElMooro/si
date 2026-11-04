# ops 6737 - 13F book: sector layer live page proof in headless Chrome (read-only)

**Status:** failure  
**Duration:** 1.1s  
**Finished:** 2026-11-04T18:35:34+00:00  

## Error

```
SystemExit: 1
```

## Log
## 0. served HTML + book over the public route

- `18:35:33` 13f.html: 100115 bytes, smart-money markup False
- `18:35:34` data/13f-book.json: HTTP 200, 1820926 bytes, cache-control public, max-age=60, s-maxage=300
- `18:35:34` book 1.2.0 13f-book.v2 generated_at 2026-11-04T18:33:58+00:00 | master 8835/9059 resolved (99.6% of value) | stance cohort/all bullish 1455 bearish 1308 | rotation buckets 13
- `18:35:34` ✗ the served 13f.html does not carry the smart-money sections yet (Pages deploy pending?)
