# ops 3404 — CW diagnosis

**Status:** success  
**Duration:** 84.9s  
**Finished:** 2026-07-17T21:41:14+00:00  

## Error

```
SystemExit: 0
```

## Log
- `21:41:13` composer err=None payload={"statusCode": 200, "body": "{\"ok\": true, \"mode\": \"PROVEN\", \"n\": 40, \"nav\": 100.0003}"}
- `21:41:14` justhodl-proven-portfolio | START RequestId: b2cbc622-68ba-4949-ac07-28459dcd37b9 Version: $LATEST || [proven-portfolio] mode=PROVEN types=70 book=391 nav=100.0002 spy_nav=100.0 83.7s || END RequestId: b2cbc622-68ba-4949-ac07-28459dcd37b9 || REPORT RequestId: b2cbc622-68ba-4949-ac07-28459dcd37b9	Duration: 83742.60 ms	Billed Duration: 84310 ms	Memory Size: 512 MB	Max Memory Used: 140 MB	Init Duration: 567.10 ms	
XRAY TraceId: 1-6a5a9f43-74ee9bf462ce851c3f49c0 || INIT_START Runtime Version: python:3.12.mainlinev2.v18	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:9819e0b13863c84a43e11f5c724871d909046d3cfb807eeb19460c63f974f26f || START RequestId: c026c698-62b4-49e9-a5b8-63e63ea3a728 Version: $LATEST
- `21:41:14` justhodl-best-setups | REPORT RequestId: 701609e6-01ed-4141-87e1-969bd247691c	Duration: 4524.95 ms	Billed Duration: 5090 ms	Memory Size: 512 MB	Max Memory Used: 154 MB	Init Duration: 564.85 ms	
XRAY TraceId: 1-6a5a9a34-592cf7941796430c29a6d59b || INIT_START Runtime Version: python:3.12.mainlinev2.v18	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:9819e0b13863c84a43e11f5c724871d909046d3cfb807eeb19460c63f974f26f || START RequestId: 42c63463-686e-4853-9636-78ff7e4e3181 Version: $LATEST || [best-setups] 534 setups · 3 strong / 13 buy · weights=prior-only · 6.2s || END RequestId: 42c63463-686e-4853-9636-78ff7e4e3181 || REPORT RequestId: 42c63463-686e-4853-9636-78ff7e4e3181	Duration: 6317.03 ms	Billed Duration: 6821 ms	Memory Size: 512 MB	Max Memory Used: 170 MB	Init Duration: 503.35 ms	
XRAY TraceId: 1-6a5aa014-2eebd148397c44932a478cbb
