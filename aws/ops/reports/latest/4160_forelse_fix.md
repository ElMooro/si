# ops 4158 — indices/futures/crypto + source-map v3

**Status:** failure  
**Duration:** 762.9s  
**Finished:** 2026-07-31T15:36:13+00:00  

## Error

```
SystemExit: 1
```

## Data

| already_fresh | artifact_gen | feed_symbol_rows | resolved | retired | sf_err | sf_out | source_map_n | targets | total_live | tv_attributed |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | None | {"ok": 246, "err": 0, "resolved": 1030} |  |  |  |  |
|  |  |  | 1030 |  |  |  |  | 1842 |  |  |
| False | 2026-07-31T14:57:10 |  |  |  |  |  |  |  |  |  |
|  |  | 436 |  |  |  |  |  |  | 3784 |  |
|  |  |  |  |  |  |  | 12103 |  |  | 2044 |
|  |  |  |  | [] |  |  |  |  |  |  |

## Log
- `15:23:40` ✅   justhodl-symbol-feed settled at loop 1
- `15:27:51`   spot ES1!: 7470.25 ysym=ES=F
- `15:36:12` ✅   post-invoke artifact after ~495s
- `15:36:13` ✅   feed v1.3 settled
- `15:36:13` ✅   targets >= 1800
- `15:36:13` ✗   resolved >= 1080
- `15:36:13` ✅   spot ES1! > 1000
- `15:36:13` ✗   feed:symbol >= 450
- `15:36:13` ✗   total LIVE >= 3800
- `15:36:13` ✅   source-map v3 written >= 10000
- `15:36:13` ✗ FAILED: ['resolved >= 1080', 'feed:symbol >= 450', 'total LIVE >= 3800']
