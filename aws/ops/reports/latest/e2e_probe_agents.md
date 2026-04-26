# E2E probe — /agent/<key> through Cloudflare Worker

**Status:** success  
**Duration:** 40.8s  
**Finished:** 2026-04-26T11:55:22+00:00  

## Log
## 📡 /agent/volatility

- `11:54:49`   ✅ HTTP 200 len=2344
- `11:54:49`   (preview not parseable JSON, len=2344B)
## 📡 /agent/dollar

- `11:54:54`   ✅ HTTP 200 len=2999
- `11:54:54`   (preview not parseable JSON, len=2999B)
## 📡 /agent/bonds

- `11:54:57`   ✅ HTTP 200 len=236
- `11:54:57`   top-level keys: ['timestamp', 'bond_indices', 'analysis', 'recommendations']
## 📡 /agent/bea

- `11:55:06`   ✅ HTTP 200 len=173510
- `11:55:06`   (preview not parseable JSON, len=173510B)
## 📡 /agent/manufacturing

- `11:55:13`   ✅ HTTP 200 len=2437
- `11:55:13`   (preview not parseable JSON, len=2437B)
## 📡 /agent/banking

- `11:55:18`   ✅ HTTP 200 len=1669
- `11:55:18`   (preview not parseable JSON, len=1669B)
## 📡 /agent/trends

- `11:55:19`   ✅ HTTP 200 len=3674
- `11:55:19`   (preview not parseable JSON, len=3674B)
## 📡 /agent/sentiment

- `11:55:20`   ✅ HTTP 200 len=53
- `11:55:20`   top-level keys: ['from_cache', 'age_hours', 'count']
## 📡 /agent/secretary

- `11:55:21`   ✅ HTTP 200 len=136301
- `11:55:21`   (preview not parseable JSON, len=136301B)
## 📡 /agent/macro-brief

- `11:55:22` ⚠   ✗ status=500 
- `11:55:22`   body: <html><body><h1>Error</h1><p>unhashable type: 'dict'</p></body></html>
## SUMMARY

- `11:55:22`   🟢 /agent/volatility     OK-RAW
- `11:55:22`   🟢 /agent/dollar         OK-RAW
- `11:55:22`   🟢 /agent/bonds          OK
- `11:55:22`   🟢 /agent/bea            OK-RAW
- `11:55:22`   🟢 /agent/manufacturing  OK-RAW
- `11:55:22`   🟢 /agent/banking        OK-RAW
- `11:55:22`   🟢 /agent/trends         OK-RAW
- `11:55:22`   🟢 /agent/sentiment      OK
- `11:55:22`   🟢 /agent/secretary      OK-RAW
- `11:55:22`   🔴 /agent/macro-brief    FAIL-500
- `11:55:22` 
  9/10 agents working through Worker
- `11:55:22` Done
