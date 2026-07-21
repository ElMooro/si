# ops 3617 — KR news-tape flash + TW via edge

**Status:** success  
**Duration:** 394.2s  
**Finished:** 2026-07-21T02:30:20+00:00  

## Error

```
SystemExit: 0
```

## Log
- `02:23:46`   zip: 88412 bytes
## 1. Lambda

- `02:23:46`   Lambda exists — updating
- `02:23:52` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `02:23:52`   invoking justhodl-asia-leads…
- `02:24:11` ✅   ✓ smoke test passed
- `02:24:11`     ok                       True
- `02:24:11`     kr_yoy                   47.96
- `02:24:11`     tw_yoy                   48.33
- `02:24:18` G1_kr_tape False
- `02:24:18` G2_tw_edge True
- `02:30:20` G3_page_row False
- `02:30:20` VERDICT: GAPS: G1_kr_tape,G3_page_row
