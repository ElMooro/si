# ops 3592 — Korea 20-day flash (KCS via CF-edge /gov)

**Status:** success  
**Duration:** 512.2s  
**Finished:** 2026-07-20T19:54:20+00:00  

## Error

```
SystemExit: 0
```

## Log
- `19:53:56` FAIL  G1_edge_breakthrough — status=200 host_hdr=www.customs.go.kr len=182 kr_str=False
- `19:53:57` moea_via_edge: {"status": 200, "len": 50000}
- `19:53:57`   zip: 86588 bytes
## 1. Lambda

- `19:53:58`   Lambda exists — updating
- `19:54:03` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `19:54:03`   invoking justhodl-asia-leads…
- `19:54:14` ✅   ✓ smoke test passed
- `19:54:14`     ok                       True
- `19:54:14`     kr_yoy                   47.96
- `19:54:14`     tw_yoy                   48.33
- `19:54:20` FAIL  G2_flash_real — v1.3.0 period='' total=$NoneB None% YoY · semis=$NoneB None% err=no 수출입 현황 item on board raw=''
- `19:54:20` PASS  G3_page_row — served: Korea 20-day flash row
- `19:54:20` VERDICT: GAPS: G1_edge_breakthrough,G2_flash_real
