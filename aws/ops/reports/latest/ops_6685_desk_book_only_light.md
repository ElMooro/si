# ops 6685 - equity-research after the point-scan rule: six requests, one container (read-only)

**Status:** success  
**Duration:** 37.2s  
**Finished:** 2026-10-26T21:09:34+00:00  

## Log
- `21:08:57` deployed: 1024 MB / 300 s, modified 2026-10-26T21:05:22
- `21:09:03` AAPL: status 200 (200), 58839 bytes in 5.3s, FunctionError None
- `21:09:05` DOCMF: status 200 (200), 47841 bytes in 2.7s, FunctionError None
- `21:09:06` ALPN: status 404 (200), 156 bytes in 0.6s, FunctionError None
- `21:09:09` MSFT: status 200 (200), 57399 bytes in 2.8s, FunctionError None
- `21:09:09` VINO: status 404 (200), 156 bytes in 0.7s, FunctionError None
- `21:09:12` INNV: status 200 (200), 52798 bytes in 2.9s, FunctionError None
## REPORT lines (memory is the proof)

- `21:09:33` AAPL: Duration: 4229.19 ms Max Memory Used: 224 MB Init Duration: 503.26 ms
- `21:09:33` DOCMF: Duration: 2561.55 ms Max Memory Used: 254 MB
- `21:09:33` ALPN: Duration: 566.36 ms Max Memory Used: 254 MB
- `21:09:34` MSFT: Duration: 2663.47 ms Max Memory Used: 258 MB
- `21:09:34` VINO: Duration: 583.20 ms Max Memory Used: 258 MB
- `21:09:34` INNV: Duration: 2775.82 ms Max Memory Used: 266 MB
## engine / shim lines

- `21:09:34`    21:08:58 | [research] fetching AAPL
- `21:09:34`    21:08:59 | [fmp-bankfirst] first hit: hits=1 stale=0 misses=0 unshaped=0 passthrough=1 bypassed=0 errors=0 bank_time=0.4s
- `21:09:34`    21:08:59 | [research] 20/43 endpoints returned data
- `21:09:34`    21:09:03 | [research] fetching DOCMF
- `21:09:34`    21:09:03 | [fmp-bankfirst] hits=26 stale=3 misses=16 unshaped=3 passthrough=13 bypassed=0 errors=0 bank_time=3.9s
- `21:09:34`    21:09:03 | [research] 14/43 endpoints returned data
- `21:09:34`    21:09:05 | [research] fetching ALPN
- `21:09:34`    21:09:06 | [research] 0/43 endpoints returned data
- `21:09:34`    21:09:06 | [research] fetching MSFT
- `21:09:34`    21:09:07 | [research] 16/43 endpoints returned data
- `21:09:34`    21:09:09 | [research] fetching VINO
- `21:09:34`    21:09:09 | [research] 0/43 endpoints returned data
- `21:09:34`    21:09:09 | [research] fetching INNV
- `21:09:34`    21:09:10 | [fmp-bankfirst] hits=62 stale=9 misses=121 unshaped=27 passthrough=40 bypassed=0 errors=0 bank_time=10.3s[fmp-bankfirst] hits=63 stale=9 misses=121 unshaped=27 passthrough=40 bypassed=0 errors=0 bank_time=10.3s
- `21:09:34`    21:09:10 | [fmp-bankfirst] hits=65 stale=9 misses=122 unshaped=27 passthrough=40 bypassed=0 errors=0 bank_time=10.5s[fmp-bankfirst] hits=64 stale=9 misses=121 unshaped=27 passthrough=40 bypassed=0 errors=0 bank_time=10.4s[fmp-bankfirst] hits=64 stale=9 misses=122 unshape
- `21:09:34`    21:09:10 | [research] 16/43 endpoints returned data
- `21:09:34` ✅ ops 6685 complete (read-only)
